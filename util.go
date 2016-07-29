package s3gof3r

import (
	"bytes"

	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

// convenience multipliers
const (
	_        = iota
	kb int64 = 1 << (10 * iota)
	mb
	gb
	tb
	pb
	eb
)

// Min and Max functions
func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// RespError representbs an http error response
// http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
type RespError struct {
	Code       string
	Message    string
	Resource   string
	RequestID  string `xml:"RequestId"`
	StatusCode int
}

func newRespError(r *http.Response) *RespError {
	e := new(RespError)
	e.StatusCode = r.StatusCode
	b, _ := ioutil.ReadAll(r.Body)
	xml.NewDecoder(bytes.NewReader(b)).Decode(e) // parse error from response
	r.Body.Close()
	return e
}

func (e *RespError) Error() string {
	return fmt.Sprintf(
		"%d: %q",
		e.StatusCode,
		e.Message,
	)
}

func checkClose(c io.Closer, err error) {
	if c != nil {
		cerr := c.Close()
		if err == nil {
			err = cerr
		}
	}

}

// A bytes.Reader that can be focefully stopped
// Also can report it's progress
type readerWrapper struct {
	*bytes.Reader
	closed    bool
	bytesdone int64
}

func (r *readerWrapper) ForceClose() {
	r.closed = true
}

func (r *readerWrapper) BytesDone() int64 {
	return r.bytesdone
}

func (r *readerWrapper) Seek(offset int64, whence int) (abs int64, err error) {
	abs, err = r.Seek(offset, whence)
	r.bytesdone = abs
	return
}

func (r *readerWrapper) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, io.ErrUnexpectedEOF
	}
	n, err = r.Reader.Read(p)
	r.bytesdone += int64(n)
	return
}
