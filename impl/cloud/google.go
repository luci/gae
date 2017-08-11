// Copyright 2017 The LUCI Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cloud

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	infoS "go.chromium.org/gae/service/info"

	"go.chromium.org/luci/common/clock"
	"go.chromium.org/luci/common/data/caching/lru"
	"go.chromium.org/luci/common/errors"

	iamAPI "google.golang.org/api/iam/v1"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// infoAccessTokenKey is a normalized string of service accounts, used as
// a key in the service accounts cache.
//
// See AccessToken for more information.
type infoAccessTokenKey string

const (
	// accessTokenRefreshPadding is the amount of time before an access token's
	// declared expiry that we remove it from our cache. This helps ensure that
	// a token that's about to expire isn't returned and used, only to become
	// invalidated mid-use.
	accessTokenRefreshPadding = 5 * time.Minute

	// publicCertificatesCacheExpiration is the expiration period for cached
	// service account public certificates.
	publicCertificatesCacheExpiration = 1 * time.Hour

	// defaultGoogleServicesCacheSize is the default maximum number of elements
	// that the LRU cache will hold.
	defaultGoogleServicesCacheSize = 1024 * 16
)

var (
	infoPublicCertificatesKey = "cloud.Info Public Certificates"
)

// GoogleServiceProvider is a ServiceProvider implementation that uses Google
// services.
type GoogleServiceProvider struct {
	// ServiceAccount is the name of the system's service account.
	ServiceAccount string

	// Cache is the LRU cache to use to store values that are fetched from remote
	// services.
	Cache *lru.Cache
}

// PublicCertificates implements ServiceProvider's PublicCertificates using
// Google's public certificate endpoint.
func (gsp *GoogleServiceProvider) PublicCertificates(c context.Context) (certs []infoS.Certificate, err error) {
	// Lock around our certificates. If they are already resolved, then we can
	// quickly return them; otherwise, we will need to load them. This lock
	// prevents concurrent certificate accesses from resulting in multiple
	// remote resource requests.
	v, err := gsp.Cache.GetOrCreate(c, &infoPublicCertificatesKey, func() (interface{}, time.Duration, error) {
		// Request a certificate map from the Google x509 public certificte endpoint.
		//
		// Upon success, the result will be a map of key to PEM-encoded value.
		url := fmt.Sprintf("https://www.googleapis.com/robot/v1/metadata/x509/%s", gsp.ServiceAccount)
		resp, err := http.Get(url)
		if err != nil {
			return nil, 0, errors.Annotate(err, "could not send request to %s", url).Err()
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, 0, errors.Annotate(err, "received HTTP %d from %s", resp.StatusCode, url).Err()
		}

		var certMap map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&certMap); err != nil {
			return nil, 0, errors.Annotate(err, "could not read/decode HTTP response body").Err()
		}

		// Populate our certificate array and sort by key for determinism.
		certs := make([]infoS.Certificate, 0, len(certMap))
		for key, data := range certMap {
			certs = append(certs, infoS.Certificate{
				KeyName: key,
				Data:    []byte(data),
			})
		}
		sort.Slice(certs, func(i, j int) bool { return certs[i].KeyName < certs[j].KeyName })
		return certs, 0, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]infoS.Certificate), nil
}

// AccessToken implements ServiceProvider's AccessToken API using the default
// Google access token source.
//
// Access tokens for a set of scopes are cached.
func (gsp *GoogleServiceProvider) AccessToken(c context.Context, scopes ...string) (*oauth2.Token, error) {
	// Normalize "scopes", removing duplicates and sorting them. This will create
	// an optimal deterministic key for a given set of scopes, regardless of their
	// order.
	scopesMap := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		scopesMap[scope] = struct{}{}
	}
	scopes = make([]string, 0, len(scopesMap))
	for scope := range scopesMap {
		scopes = append(scopes, scope)
	}
	sort.Strings(scopes)
	cacheKey := infoAccessTokenKey(strings.Join(scopes, "\x00"))

	now := clock.Now(c)

	tok, err := gsp.Cache.GetOrCreate(c, cacheKey, func() (interface{}, time.Duration, error) {
		ts, err := google.DefaultTokenSource(c, scopes...)
		if err != nil {
			return nil, 0, err
		}
		tok, err := ts.Token()
		if err != nil {
			return nil, 0, err
		}

		// Determine when we should invalidate this token in our cache.
		exp := tok.Expiry.Sub(now)
		if exp <= 0 {
			return nil, 0, errors.New("returned expired access token")
		}
		if exp > accessTokenRefreshPadding {
			exp -= accessTokenRefreshPadding
		} else {
			// Expiration is below our refresh padding, so refresh halfway through its
			//lifecycle.
			exp /= 2
		}
		return tok, exp, nil
	})
	if err != nil {
		return nil, err
	}
	return tok.(*oauth2.Token), nil
}

// SignBytes implements ServiceProvider's SignBytes using Google Cloud IAM's
// "SignBlob" endpoint.
//
// This must be an authenticated call.
//
// https://cloud.google.com/iam/reference/rest/v1/projects.serviceAccounts/signBlob
func (gsp *GoogleServiceProvider) SignBytes(c context.Context, bytes []byte) (keyName string, signature []byte, err error) {
	// Generate a client to use for the SignBytes API call.
	var tok *oauth2.Token
	if tok, err = gsp.AccessToken(c, iamAPI.CloudPlatformScope); err != nil {
		return
	}
	client := oauth2.NewClient(c, oauth2.StaticTokenSource(tok))

	// Construct an IAM service.
	var svc *iamAPI.Service
	if svc, err = iamAPI.New(client); err != nil {
		err = errors.Annotate(err, "could not get IAM client").Err()
		return
	}

	var resp *iamAPI.SignBlobResponse
	req := svc.Projects.ServiceAccounts.SignBlob(
		fmt.Sprintf("projects/-/serviceAccounts/%s", gsp.ServiceAccount),
		&iamAPI.SignBlobRequest{
			BytesToSign: base64.StdEncoding.EncodeToString(bytes),
		})
	resp, err = req.Context(c).Do()
	if err != nil {
		err = errors.Annotate(err, "SignBlob RPC failed").Err()
		return
	}

	keyName = resp.KeyId
	signature = []byte(resp.Signature)
	return
}
