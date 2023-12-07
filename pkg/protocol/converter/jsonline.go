package protocol

import (
	"bytes"
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
	serializedLogs := make([][]byte, len(convertedLogs))
	for i, log := range convertedLogs {
		switch c.Encoding {
		case EncodingJSON:
			b, err := marshalWithoutHTMLEscaped(log)
			if err != nil {
				return nil, nil, fmt.Errorf("unable to marshal log: %v", log)
			}
			serializedLogs[i] = b
		default:
			return nil, nil, fmt.Errorf("unsupported encoding format: %s", c.Encoding)
		}
	}
	return bytes.Join(serializedLogs, sep), nil, nil
}
