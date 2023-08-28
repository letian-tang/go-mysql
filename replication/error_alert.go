package replication

const (
	chanLen = 40
)

var (
	alertChan = make(chan string, chanLen)
)

func GetAlert() chan string {
	return alertChan
}

func AddAlert(alert string) {
	//如果来不及消费，丢弃后续通知
	if len(alertChan) < chanLen {
		alertChan <- alert
	}
}
