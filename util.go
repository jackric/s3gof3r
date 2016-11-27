package s3gof3r

import (
	"bytes"
	"time"

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
	io.Reader
	closed    *bool
	bytesdone *int64
}

func newReaderWrapper(r io.Reader) *readerWrapper {
	var x = int64(0)
	var y = bool(false)
	return &readerWrapper{Reader: r, closed: &y, bytesdone: &x}
}

func (r readerWrapper) ForceClose() {
	*r.closed = true
}

func (r readerWrapper) BytesDone() int64 {
	return *r.bytesdone
}

func (r readerWrapper) Seek(offset int64, whence int) (abs int64, err error) {
	abs, err = r.Seek(offset, whence)
	*r.bytesdone = abs
	return
}

func (r readerWrapper) Read(p []byte) (n int, err error) {
	if *r.closed {
		return 0, io.ErrUnexpectedEOF
	}
	n, err = r.Reader.Read(p)
	*r.bytesdone += int64(n)
	return
}

// A bytes.Reader that can be focefully stopped
// Also can report it's progress
type seekReaderWrapper struct {
	io.ReadSeeker
	closed    *bool
	bytesdone *int64
}

func newSeekReaderWrapper(r io.ReadSeeker) *seekReaderWrapper {
	var x = int64(0)
	var y = bool(false)
	return &seekReaderWrapper{ReadSeeker: r, closed: &y, bytesdone: &x}
}

func (r seekReaderWrapper) ForceClose() {
	*r.closed = true
}

func (r seekReaderWrapper) BytesDone() int64 {
	return *r.bytesdone
}

func (r seekReaderWrapper) Seek(offset int64, whence int) (abs int64, err error) {
	abs, err = r.ReadSeeker.Seek(offset, whence)
	*r.bytesdone = abs
	return
}

func (r seekReaderWrapper) Read(p []byte) (n int, err error) {
	if *r.closed {
		return 0, io.ErrUnexpectedEOF
	}
	n, err = r.ReadSeeker.Read(p)
	*r.bytesdone += int64(n)
	return
}

type speedTracker struct {
	Speed    int64
	lastTime time.Time
	lastDone int64
}

func newSpeedTracker() *speedTracker {
	return &speedTracker{lastTime: time.Now()}
}

func (s *speedTracker) update(bytesNow int64) (speed int64) {
	newBytes := bytesNow - s.lastDone
	timeNow := time.Now()
	elapsed := timeNow.Sub(s.lastTime)
	elapsedSeconds := elapsed.Seconds()

	newSpeed := int64(float64(newBytes) / elapsedSeconds)
	s.Speed = rollingAvg(s.Speed, newSpeed)
	return s.Speed
}

func rollingAvg(existing int64, new int64) int64 {
	smoothingFactor := 0.05
	return int64((smoothingFactor * float64(new)) + ((1 - smoothingFactor) * float64(existing)))
}
