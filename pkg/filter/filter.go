package filter

type IClaimFunc func(topic string, msg []byte) ([]byte, error)
