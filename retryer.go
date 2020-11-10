package dasql

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

// Retryer is a custom retryer that retryes even when the Data API returns a Bad request with
// the underlying message of "Communications link failure". As of November 10th 2020 this error
// is returned by the Data API when the database cluster is asleep and the retryer is required
// to force the SDK to retry while the cluster is waking up.
type Retryer struct{ client.DefaultRetryer }

// ShouldRetry implements the retyer interface
func (re Retryer) ShouldRetry(r *request.Request) bool {
	if brerr, ok := r.Error.(*rdsdataservice.BadRequestException); ok &&
		strings.HasPrefix(brerr.Message(), "Communications link failure") {
		return true // should retry error that occurs when the aurora db is waking up
	}

	return re.DefaultRetryer.ShouldRetry(r)
}
