package commons


type MessageLike interface {
	AppendHeader(key string, value string)
	GetHeaders() []MessageHeaderLike
	GetBody() []byte
	GetBodyAsString() string
	SetBody(body []byte)
	SetBodyFromString(body string)

}