package websocket

import (
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
  "mykafka/kafka"
)

var upgrader = websocket.Upgrader{}

func SocketHandler(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true } //never blindly trust any origin. But since test ok
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Ws connection error", err)
	}
	defer ws.Close()
	for { //make this a goroutine...
		msg,ok:= <- kafka.ReadChannel
    if ok==false{
      fmt.Println("Channel Close ", ok)
      break
    }
		fmt.Println(msg)
		ws.WriteMessage(websocket.TextMessage, msg) //handleIncomingMessage(websocket.TextMessage,msg)
	}
}
