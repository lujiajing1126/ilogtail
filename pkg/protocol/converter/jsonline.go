package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/alibaba/ilogtail/pkg/protocol"
)

var (
	sep = []byte("\n")
)

func (c *Converter) ConvertToJsonlineProtocolStreamFlatten(logGroup *protocol.LogGroup) ([]byte, []map[string]string, error) {
	convertedLogs, _, err := c.ConvertToSingleProtocolLogsFlatten(logGroup, nil)
	if err != nil {
		return nil, nil, err
	}
	joinedStream := *GetPooledByteBuf()
	for _, log := range convertedLogs {
		switch c.Encoding {
		case EncodingJSON:
			err := marshalWithoutHTMLEscapedWithoutAlloc(log, joinedStream)
			if err != nil {
				// release byte buffer
				PutPooledByteBuf(&joinedStream)
				return nil, nil, fmt.Errorf("unable to marshal log: %v", log)
			}
			// trim and append a \n
			joinedStream = append(trimRightByte(joinedStream, sep[0]), sep[0])
		default:
			return nil, nil, fmt.Errorf("unsupported encoding format: %s", c.Encoding)
		}
	}
	return joinedStream, nil, nil
}

func marshalWithoutHTMLEscapedWithoutAlloc(data interface{}, dst []byte) error {
	jsonEncoder := json.NewEncoder(bytes.NewBuffer(dst))
	jsonEncoder.SetEscapeHTML(false)
	if err := jsonEncoder.Encode(data); err != nil {
		return err
	}
	return nil
}

func trimRightByte(s []byte, c byte) []byte {
	for len(s) > 0 && s[len(s)-1] == c {
		s = s[:len(s)-1]
	}
	return s
}
