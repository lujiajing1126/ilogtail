package protocol

import (
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
			b, err := marshalWithoutHTMLEscaped(log)
			if err != nil {
				return nil, nil, fmt.Errorf("unable to marshal log: %v", log)
			}
			joinedStream = append(joinedStream, b...)
			// reset bytes
			b = b[:0]
			b = nil
			joinedStream = append(joinedStream, sep...)
		default:
			return nil, nil, fmt.Errorf("unsupported encoding format: %s", c.Encoding)
		}
	}
	return joinedStream, nil, nil
}
