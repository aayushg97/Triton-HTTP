package tritonhttp

import (
	"log"
	"fmt"
	"net"
	"os"
	"bufio"
	"time"
	"errors"
	"io"
	"strings"
	"path/filepath"
	"bytes"
	"strconv"
	"sort"
)

const (
	responseProto = "HTTP/1.1"
	statusOK				= 200
	statusBadRequest		= 400
	statusNotFound			= 404
	logging					= true
	READ_TIMEOUT time.Duration = 5 * time.Second
)

var statusText = map[int]string{
	statusOK:               "OK",
	statusBadRequest:		"Bad Request",
	statusNotFound:			"Not Found",
}

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"

	// VirtualHosts contains a mapping from host name to the docRoot path
	// (i.e. the path to the directory to serve static files from) for
	// all virtual hosts that this server supports
	VirtualHosts map[string]string
}

func LogMessage(a ...any) {
	if(logging){
		fmt.Println(a)
	}
}

func (s *Server) ValidateServerSetup() error {
	
	for _, docRoot := range s.VirtualHosts {
		fileStatus, fileErr := os.Stat(docRoot)

		if (os.IsNotExist(fileErr) || !fileStatus.IsDir()) {
			LogMessage("Either doc root does not exist or it is not a directory")
			return fileErr
		}
	}

	return nil
}

// func ReadLine(conn net.Conn) (string, error) {
// 	var line string
// 	for {
// 		s, err := br.ReadString('\n')
// 		line += s
// 		// Return the error
// 		if err != nil {
// 			fmt.Println("Error in reading string")
// 			return line, err
// 		}
// 		// Return the line when reaching line end
// 		if strings.HasSuffix(line, "\r\n") {
// 			// Striping the line end
// 			line = line[:len(line)-2]
// 			return line, nil
// 		}
// 	}
// }

// func ReadLine(br *bufio.Reader) (line string, err error) {
// 	lineInBytes := make([]byte, 0)
// 	err = nil
// 	for {
// 		partialLine, isPrefix, lineErr := br.ReadLine()

// 		if(lineErr != nil) {
// 			err = lineErr
// 			break
// 		}

// 		lineInBytes = append(lineInBytes, partialLine...)

// 		if(isPrefix == false) {
// 			break
// 		}
// 	}

// 	line = string(lineInBytes)

// 	return line, err
// }

func ReadLine(conn net.Conn, message []byte) (string, []byte, error) {
	delim := []byte("\r\n")
	buf := make([]byte, 1024)

	for bytes.Index(message, delim) == -1 {
		bytes_read, readErr := conn.Read(buf)
		if(readErr != nil) {
			return "", message, readErr
		}

		message = append(message, buf[:bytes_read]...)
	}

	ind := bytes.Index(message, delim)
	if(ind != -1) {
		line := message[:ind]

		if(len(message)>ind+2){
			message = message[ind+2:]
		} else {
			message = make([]byte, 0)
		}

		return string(line), message, nil
	} else {
		return "", message, fmt.Errorf("Error in reading line")
	}
}

func CheckAlphaNumericandHyphen(key string) (bool) {
	for ch := range key {
		if((ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '-') {
			return false
		}
	}

	return true
}

func ParseRequestHeader(line string) (string, string, error) {
	fields := strings.SplitN(line, ":", 2)
	
	if(len(fields) != 2){
		return "", "", fmt.Errorf("Could not parse request header")
	}

	key := strings.TrimSpace(fields[0])
	value := strings.TrimSpace(fields[1])

	if (key == ""/* || !CheckAlphaNumericandHyphen(key)*/) {
		return "", "", fmt.Errorf("Invalid key in header")
	}

	key = CanonicalHeaderKey(key)

	return key, value, nil
}

func ValidateRequestHeaders(req *Request) (error) {
	_, status := req.Headers["Host"]
	if(!status) {
		return fmt.Errorf("Host header not found") 
	}

	return nil
}

func (s *Server) ReadRequest(conn net.Conn, message []byte) (*Request, []byte, error) {
	req := &Request{}
	var line string
	var readErr error

	// Read start line
	line, message, readErr = ReadLine(conn, message)
	if(readErr != nil) {
		// handle error
		if err, ok := readErr.(net.Error); ok && err.Timeout() && len(message) > 0 {
			return nil, message, fmt.Errorf("Timeout with partial request")
		} else {
			LogMessage("Error in reading start line ", readErr)
			return nil, message, readErr
		}
	}

	// Populate method, URL and version
	fields := strings.SplitN(line, " ", 3)

	if len(fields) != 3 || fields[0] != "GET" || fields[1][0] != '/' || fields[2] != "HTTP/1.1" {
		return nil, message, fmt.Errorf("could not parse the request line")
	}

	req.Method = fields[0]
	req.Proto = fields[2]

	// Read headers
	req.Headers = make(map[string]string)

	for {
		// Read header line
		line, message, readErr = ReadLine(conn, message)
		if(readErr != nil) {
			// handle error
			if err, ok := readErr.(net.Error); ok && err.Timeout() {
				return nil, message, fmt.Errorf("Timeout with partial request")
			} else {
				LogMessage("Error in reading header line")
				return nil, message, readErr
			}
		}

		if len(line) == 0 || line == "" { // header end
			LogMessage("Header has ended")
			break
		} else {
			// Parse header line
			key, value, parseErr := ParseRequestHeader(line)
			if(parseErr != nil) {
				return nil, message, parseErr
			}

			req.Headers[key] = value
		}
	}

	// Validate request headers
	headerErr := ValidateRequestHeaders(req)

	if(headerErr != nil) {
		return nil, message, headerErr 
	}

	req.Host = req.Headers["Host"]
	req.Close = req.Headers["Connection"] == "close"

	// Modify URL if required
	pathToFile := filepath.Join(s.VirtualHosts[req.Headers["Host"]], fields[1])
	fileStatus, fileErr := os.Stat(pathToFile)

	if (fileErr == nil) {
		if (fileStatus.IsDir() || fields[1][len(fields[1])-1] == '/') {
			fields[1] = filepath.Join(fields[1], "index.html")
		}
	}

	req.URL = fields[1]

	return req, message, nil
}

// func ParseAndValidateRequestLine(line string) (string, error) {
// 	fields := strings.SplitN(line, " ", 3)
// 	if len(fields) != 3 {
// 		return "", fmt.Errorf("could not parse the request line")
// 	} else if(fields[0] != "GET" || ValidateURL(fields[1]) != nil || fields[2] != "HTTP/1.1") {
// 		return "", fmt.Errorf("Malformed request")
// 	}

// 	return fields[0], nil
// }

// HandleOK prepares res to be a 200 OK response
// ready to be written back to client.
func (res *Response) HandleOK() {
	res.Proto = responseProto
	res.StatusCode = statusOK
}

func (s *Server) HandleGoodRequest(req *Request) (*Response, error) {
	res := &Response{}
	res.HandleOK()
	res.StatusText = statusText[statusOK]
	res.Request = req
	res.FilePath = filepath.Join(s.VirtualHosts[req.Headers["Host"]], req.URL)

	// Populate headers in sorted order
	res.Headers = make(map[string]string)

	if(req.Headers["Connection"] == "close") {
		res.Headers["Connection"] = "close"
	}

	body, err := os.ReadFile(res.FilePath)
	if err != nil {
		LogMessage("Error in reading file ", res.FilePath)
		return nil, err
	}

	res.Headers["Content-Length"] = strconv.Itoa(len(body))
	res.Headers["Content-Type"] = MIMETypeByExtension(filepath.Ext(res.FilePath))
	res.Headers["Date"] = FormatTime(time.Now())

	fileStat, err := os.Stat(res.FilePath)
	if(err != nil) {
		LogMessage("Error in getting file stats")
		return nil, err
	}
	
	res.Headers["Last-Modified"] = FormatTime(fileStat.ModTime())

	return res, nil
}

// HandleBadRequest prepares res to be a 400 Method Not allowed response
func (res *Response) HandleBadRequest() {
	res.Proto = responseProto
	res.StatusCode = statusBadRequest
	res.FilePath = ""
	res.Headers = make(map[string]string)
	res.Headers["Connection"] = "close"
	res.Headers["Date"] = FormatTime(time.Now())
}

func (s *Server) FileExistsInDocRoot(req *Request) (bool) {
	pathToFile := filepath.Join(s.VirtualHosts[req.Headers["Host"]], req.URL)

	if(!strings.HasPrefix(pathToFile, s.VirtualHosts[req.Headers["Host"]])) {
		LogMessage("Attempt to escape doc root")
		return false
	}

	if _, err := os.Stat(pathToFile); err != nil {
		LogMessage("Error in getting file stat: ", err)
		return false
	}

	return true
}

func (res *Response) HandleNotFound(req *Request) {
	res.Proto = responseProto
	res.StatusCode = statusNotFound
	res.FilePath = ""
	res.Headers = make(map[string]string)
	if(req.Headers["Connection"] == "close") {
		res.Headers["Connection"] = "close"
	}
	res.Headers["Date"] = FormatTime(time.Now())
}

func (res *Response) Write(w io.Writer) error {
	bw := bufio.NewWriter(w)

	statusLine := fmt.Sprintf("%v %v %v\r\n", res.Proto, res.StatusCode, statusText[res.StatusCode])
	if _, err := bw.WriteString(statusLine); err != nil {
		return err
	}

	if(res.Headers != nil) {
		// Sort the headers
		keys := make([]string, 0, len(res.Headers))
		
		for k := range res.Headers {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		// Write the headers
		for _, k := range keys {
			headerLine := fmt.Sprintf("%v: %v\r\n", k, res.Headers[k])
			if _, err := bw.WriteString(headerLine); err != nil {
				return err
			}
		}
	}

	emptyLine := fmt.Sprintf("\r\n")
	if _, err := bw.WriteString(emptyLine); err != nil {
		return err
	}

	if(res.StatusCode == statusOK) {
		body, err := os.ReadFile(res.FilePath)
		if err != nil {
			return err
		}

		if _, err := bw.Write(body); err != nil {
			return err
		}
	}

	if err := bw.Flush(); err != nil {
		return nil
	}

	return nil
}

func (s *Server) HandleClient(conn net.Conn) {
	// message array for this client
	message := make([]byte, 0)
	
	// read the next request
	for {
		// Set a read timeout
		if err := conn.SetReadDeadline(time.Now().Add(READ_TIMEOUT)); err != nil {
			LogMessage("Error in setting read deadline: ", err)
			_ = conn.Close()
			return
		}

		var req *Request
		var err error
		// Read next request from the client
		req, message, err = s.ReadRequest(conn, message)
		// if err != nil {
		// 	return
		// }

		// handle errors

		// error 1: client has closed the conn => io.EOF error
		if errors.Is(err, io.EOF) {
			LogMessage("Closing connection due to EOF")
			_ = conn.Close()
			return
		}

		// error 2: timeout from the server --> net.Error
		// timeout in this application means we just close the connection
		// Note : proj3 might require you to do a bit more here
		if localErr, ok := err.(net.Error); ok && localErr.Timeout() {
			LogMessage("Closing connection due to timeout")
			_ = conn.Close()
			return
		}

		// error 3: malformed/invalid request
		// Handle the request which is not a GET and immediately close the connection and return
		if err != nil {
			res := &Response{}
			res.HandleBadRequest()
			_ = res.Write(conn)
			_ = conn.Close()
			LogMessage(err)
			return
		}

		// Check for 404 errors
		// does file at url exist
		if !s.FileExistsInDocRoot(req) {
			LogMessage("File not found")
			res := &Response{}
			res.HandleNotFound(req)
			_ = res.Write(conn)
			LogMessage("Not found request handled")

			if(res.Headers["Connection"] == "close") {
				LogMessage("Closing connection due to connection close header in not found request")
				_ = conn.Close()
				return
			}
		} else {
			// Handle good request
			log.Println("Handling good request")
			res, err := s.HandleGoodRequest(req)

			if(err != nil) {
				LogMessage(err)
			}

			err = res.Write(conn)
			if err != nil {
				LogMessage(err)
			}

			LogMessage("Good request handled")

			if(res.Headers["Connection"] == "close") {
				LogMessage("Closing connection due to connection close header")
				_ = conn.Close()
				return
			}
		}

		// We'll never close the connection and handle as many requests for this connection and pass on this
		// responsibility to the timeout mechanism
	}
}

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.
func (s *Server) ListenAndServe() error {
	// Hint: Validate all docRoots
	// Validate the configuration of the server
	if validationErr := s.ValidateServerSetup(); validationErr != nil {
		LogMessage("Error in server validation: ", validationErr)
		return validationErr
	}

	// Hint: create your listen socket and spawn off goroutines per incoming client
	listener, listenErr := net.Listen("tcp", s.Addr)
	if(listenErr != nil) {
		LogMessage("Error in listen: ", listenErr)
		return listenErr
	}

	defer listener.Close()

	// Accept incoming connections
	for {
		conn, connErr := listener.Accept()

		if connErr != nil {
			LogMessage("Error in accepting connection: ", connErr)
			continue
		}

		go s.HandleClient(conn)
	}
}
