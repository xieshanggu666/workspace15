extends Node
## 权威服务端 WebSocket(JSON)客户端。
## 只做传输与本地事件序号跟踪; 不实现任何结算规则。

signal connected_ok(token)
signal login_failed(code)
signal matched(match_id, seat)
signal resumed(match_id, seat)
signal snapshot_received(state, deadline_at)
signal event_received(seq, event)
signal match_ended(winner, reason, state_hash)
signal command_ack(op_id, accepted, duplicate, payload)
signal replay_received(match_id, events, full)
signal connection_status(text)

var _ws: WebSocketPeer = WebSocketPeer.new()
var url := "ws://127.0.0.1:8765"
var token := ""
var last_seq := 0
var _poll := false
var _op_counter := 0

func connect_to(server_url: String, player_name: String, saved_token: String = "") -> void:
	url = server_url
	token = saved_token
	var err := _ws.connect_to_url(url)
	if err != OK:
		connection_status.emit("连接失败: %d" % err)
		return
	_poll = true
	set_process(true)
	var msg := {"type": "login", "name": player_name}
	if saved_token != "":
		msg["token"] = saved_token
	# 等连接建立后在 _process 里发送首条消息
	_pending_login = msg

var _pending_login: Dictionary = {}

func _process(_delta: float) -> void:
	if not _poll:
		return
	_ws.poll()
	var state := _ws.get_ready_state()
	if state == WebSocketPeer.STATE_OPEN and not _pending_login.is_empty():
		_send_dict(_pending_login)
		_pending_login = {}
	while _ws.get_available_packet_count() > 0:
		var raw := _ws.get_packet().get_string_from_utf8()
		_handle(JSON.parse_string(raw))
	if state == WebSocketPeer.STATE_CLOSED:
		_poll = false
		connection_status.emit("连接已断开")

func _handle(msg: Variant) -> void:
	if typeof(msg) != TYPE_DICTIONARY:
		return
	match msg.get("type", ""):
		"login_ok":
			token = msg.get("token", token)
			connected_ok.emit(token)
		"login_fail":
			login_failed.emit(msg.get("code", "?"))
		"match_begin":
			matched.emit(msg["match_id"], msg["seat"])
		"match_resume":
			resumed.emit(msg["match_id"], msg["seat"])
		"snapshot":
			snapshot_received.emit(msg["state"], msg.get("deadline_at"))
		"event":
			last_seq = int(msg["seq"])
			event_received.emit(int(msg["seq"]), msg["event"])
		"command_result":
			command_ack.emit(msg.get("op_id", ""), bool(msg.get("accepted", false)),
				bool(msg.get("duplicate", false)), msg)
		"match_end":
			match_ended.emit(msg.get("winner"), msg.get("reason"),
				msg.get("state_hash"))
		"replay":
			replay_received.emit(msg["match_id"], msg.get("events", []),
				bool(msg.get("full", false)))
		"error":
			push_warning("服务端错误: %s" % msg.get("code", "?"))
		_:
			pass

# --------------------------------------------------------------- 对外动作

func queue_with_deck(deck: Array) -> void:
	_send_dict({"type": "queue", "deck": deck})

func send_command(cmd: Dictionary) -> String:
	## 返回本次操作的 op_id(本地生成, 重发同一 op_id 不会重复扣费)
	_op_counter += 1
	var op_id := "%d-%d" % [Time.get_ticks_msec(), _op_counter]
	_send_dict({"type": "command", "op_id": op_id, "command": cmd})
	return op_id

func resend_command(op_id: String, cmd: Dictionary) -> void:
	## 断线重连后用同一 op_id 补发: 服务端若已处理则回 duplicate
	_send_dict({"type": "command", "op_id": op_id, "command": cmd})

func request_sync(since_seq: int) -> void:
	_send_dict({"type": "sync", "last_seq": since_seq})

func request_replay(match_id: String) -> void:
	_send_dict({"type": "replay", "match_id": match_id})

func close() -> void:
	_poll = false
	_ws.close()

func _send_dict(d: Dictionary) -> void:
	if _ws.get_ready_state() != WebSocketPeer.STATE_OPEN:
		push_warning("连接未建立, 消息已丢弃: %s" % d.get("type", ""))
		return
	_ws.send_text(JSON.stringify(d))
