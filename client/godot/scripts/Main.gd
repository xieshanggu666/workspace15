extends Control
## 客户端主界面: 组牌/登录、据点战场、手牌、响应窗、掉线重连与回放查看。
## 所有牌面信息来自服务端过滤后的事件/快照, 本地不推断对手手牌。

const DECK_SIZE := 15

var client: Node
var state: Variant = null
var my_seat := -1
var selected_hand: Variant = null          # 选中的手牌条目
var pending_op: String = ""                 # 最近一次未确认的 op_id(重连补发用)
var pending_cmd: Dictionary = {}
var deadline_at: float = 0.0
var token := ""
var player_name := ""
var server_url := "ws://127.0.0.1:8765"
var deck: Array = []

@onready var root: TabContainer = $Layout
@onready var log_view: RichTextLabel = $Layout/Battle/Right/Log
@onready var status_label: Label = $TopBar/Status
@onready var timer_label: Label = $TopBar/Timer
@onready var sites_container: HBoxContainer = $Layout/Battle/Center/Board/Sites
@onready var hand_container: HBoxContainer = $Layout/Battle/Center/Hand/Cards
@onready var action_button: Button = $Layout/Battle/Center/Hand/Actions/Play
@onready var pass_button: Button = $Layout/Battle/Center/Hand/Actions/Pass
@onready var end_button: Button = $Layout/Battle/Center/Hand/Actions/EndTurn
@onready var target_editor: OptionButton = $Layout/Battle/Center/Hand/Actions/SitePicker
@onready var reconnect_button: Button = $TopBar/Reconnect
@onready var replay_button: Button = $TopBar/Replay


func _ready() -> void:
	client = preload("res://scripts/Client.gd").new()
	client.name = "NetClient"
	add_child(client)
	client.snapshot_received.connect(_on_snapshot)
	client.event_received.connect(_on_event)
	client.matched.connect(_on_matched)
	client.resumed.connect(_on_resumed)
	client.match_corrupt.connect(_on_corrupt)
	client.match_ended.connect(_on_match_end)
	client.command_ack.connect(_on_ack)
	client.replay_received.connect(_on_replay)
	client.connection_status.connect(func(t): status_label.text = t)
	client.connected_ok.connect(_on_connected)
	root.current_tab = 0
	_fill_deck_editor()
	$Layout/Lobby/Connect.pressed.connect(_connect_clicked)
	action_button.pressed.connect(_play_clicked)
	pass_button.pressed.connect(_pass_clicked)
	end_button.pressed.connect(_end_clicked)
	reconnect_button.pressed.connect(_manual_reconnect)
	replay_button.pressed.connect(_request_current_replay)
	for i in range(3):
		target_editor.add_item("据点 %d" % i, i)
	target_editor.select(0)
	set_process(true)

func _refresh_target_picker() -> void:
	target_editor.clear()
	if selected_hand == null:
		for i in range(3):
			target_editor.add_item("据点 %d" % i, i)
			target_editor.set_item_metadata(i, i)
		return
	match selected_hand.get("id", ""):
		"fireball":
			for seat in range(2):
				for u in state["seats"][seat]["units"]:
					var idx := target_editor.item_count
					target_editor.add_item("%s %s (%d血)" % [
						("敌方" if seat != my_seat else "友方"), u.get("id", "?"),
						int(u.get("hp", 0))], idx)
					target_editor.set_item_metadata(idx, u.get("uid", ""))
		"reinforce", "repair":
			for u in state["seats"][my_seat]["units"]:
				var idx := target_editor.item_count
				target_editor.add_item("友方 %s (%d血)" % [u.get("id", "?"),
					int(u.get("hp", 0))], idx)
				target_editor.set_item_metadata(idx, u.get("uid", ""))
		_:
			for i in range(3):
				target_editor.add_item("据点 %d" % i, i)
				target_editor.set_item_metadata(i, i)
	if target_editor.item_count == 0:
		target_editor.add_item("(无合法目标)", 0)
		target_editor.set_item_metadata(0, "")
	target_editor.select(0)

func _select_hand(entry: Variant) -> void:
	selected_hand = entry
	_refresh_target_picker()


# ------------------------------------------------------------- 大厅/组牌

func _fill_deck_editor() -> void:
	var grid: GridContainer = $Layout/Lobby/DeckEdit/Cards
	var cards := [
		["militia", "民兵 1费 1/2"], ["infantry", "步兵 2费 2/3"],
		["vanguard", "先锋 3费 3/4"], ["guardian", "守卫 4费 3/6"],
		["fireball", "火球 2费"], ["reinforce", "增援 1费"],
		["repair", "修理 1费"], ["counter", "反制 2费"],
		["banner", "战旗 2费"], ["poison", "毒雾 2费"],
	]
	for c in cards:
		var b := Button.new()
		b.text = "+ " + c[1]
		b.pressed.connect(_add_card.bind(c[0]))
		grid.add_child(b)

func _add_card(id: String) -> void:
	if deck.size() >= DECK_SIZE:
		return
	var n := deck.count(id)
	if n >= 3:
		_log("该牌已满 3 张: %s" % id)
		return
	deck.append(id)
	$Layout/Lobby/DeckEdit/List.text = ", ".join(deck) + " (%d/%d)" % [deck.size(), DECK_SIZE]

var _want_queue := false

func _connect_clicked() -> void:
	player_name = $Layout/Lobby/NameEdit.text
	server_url = $Layout/Lobby/UrlEdit.text
	if deck.size() != DECK_SIZE:
		_log("组牌不足 %d 张" % DECK_SIZE)
		return
	_want_queue = true
	var saved: String = _load_token(player_name)
	client.connect_to(server_url, player_name, saved)

func _on_connected(t: String) -> void:
	token = t
	_save_token(player_name, token)
	if _want_queue:
		client.queue_with_deck(deck)
		_log("已加入匹配队列")
	_want_queue = false


# ------------------------------------------------------------- 网络回调

func _on_matched(match_id: String, seat: int) -> void:
	my_seat = seat
	root.current_tab = 1
	_log("对局开始 %s, 你的座位: %d" % [match_id.substr(0, 8), seat])

func _on_resumed(match_id: String, seat: int) -> void:
	my_seat = seat
	root.current_tab = 1
	_log("已恢复对局 %s" % match_id.substr(0, 8))

func _on_corrupt(match_id: String, message: String) -> void:
	# 服务端已隔离损坏对局: 清掉本地状态, 绝不在本地继续渲染/结算
	state = null
	_log("对局 %s 无法恢复: %s" % [match_id.substr(0, 8), message])
	status_label.text = "对局已隔离(事件流校验失败)"

func _on_snapshot(s: Variant, dl: Variant) -> void:
	state = s
	deadline_at = float(dl) if dl != null else 0.0
	_render()

func _on_event(_seq: int, event: Variant) -> void:
	# 事件用于细粒度动画; 渲染权威真相由紧随其后的 snapshot 驱动
	_log("事件: %s" % event.get("type", "?"))

func _on_ack(op_id: String, accepted: bool, duplicate: bool, payload: Variant) -> void:
	if op_id == pending_op:
		pending_op = ""
	if duplicate:
		_log("操作 %s 为重复请求, 服务端未重复结算" % op_id)
	elif not accepted:
		_log("被拒绝: %s" % payload.get("code", "?"))

func _on_match_end(winner: Variant, reason: String, _h: String) -> void:
	if winner == null:
		_log("对局结束: 平局(%s)" % reason)
	elif int(winner) == my_seat:
		_log("胜利! (%s)" % reason)
	else:
		_log("失败 (%s)" % reason)

func _on_replay(_mid: String, events: Array, full: bool,
		status: String = "", verify_error: Variant = null) -> void:
	_log("回放: %d 条事件 (完整=%s)" % [events.size(), str(full)])
	if status == "corrupt":
		_log("警告: 该对局已隔离, 校验错误: %s" % str(verify_error))


# ------------------------------------------------------------- 操作

func _play_clicked() -> void:
	if selected_hand == null:
		return
	var target: Variant = target_editor.get_item_metadata(target_editor.selected)
	var cmd := {"cmd": "PLAY", "uid": selected_hand.get("uid", ""), "target": target}
	pending_cmd = cmd
	pending_op = client.send_command(cmd)
	selected_hand = null
	_refresh_target_picker()

func _pass_clicked() -> void:
	pending_cmd = {"cmd": "PASS"}
	pending_op = client.send_command(pending_cmd)

func _end_clicked() -> void:
	pending_cmd = {"cmd": "END_TURN"}
	pending_op = client.send_command(pending_cmd)

func _manual_reconnect() -> void:
	if player_name == "":
		return
	client.close()
	# 服务端在 login 成功后自动下发 match_resume + 全量事件 + snapshot
	var saved := _load_token(player_name)
	client.connect_to(server_url, player_name, saved)

func _request_current_replay() -> void:
	var mid := _current_match_id()
	if mid != "":
		client.request_replay(mid)


# ------------------------------------------------------------- 渲染

func _process(_dt: float) -> void:
	if state != null and deadline_at > 0:
		var remain: float = max(0.0, deadline_at - Time.get_unix_time_from_system())
		timer_label.text = "剩余 %.1fs" % remain
		status_label.text = "阶段: %s  回合: %s" % [state.get("phase", "?"),
			str(state.get("turn", 0))]

func _render() -> void:
	if state == null:
		return
	for i in range(sites_container.get_child_count()):
		var box: PanelContainer = sites_container.get_child(i)
		var info: RichTextLabel = box.get_node("Info")
		var owner_id = state["sites"][i].get("owner")
		var owner_text := "中立" if owner_id == null else ("我方" if int(owner_id) == my_seat else "敌方")
		var lines: Array[String] = ["[b]据点 %d[/b] (%s)" % [i, owner_text]]
		for seat in range(2):
			for u in state["seats"][seat]["units"]:
				if int(u.get("site", -1)) == i:
					var who := "我" if seat == my_seat else "敌"
					lines.append("%s %s %d/%d" % [who, u.get("id", "?"),
						int(u.get("power", 0)), int(u.get("hp", 0))])
		info.text = "\n".join(lines)

	# 手牌(只有自己座位是完整列表; 对手位置不渲染牌面)
	for c in hand_container.get_children():
		c.queue_free()
	var mine: Variant = state["seats"][my_seat]["hand"]
	if typeof(mine) == TYPE_ARRAY:
		for entry in mine:
			var b := Button.new()
			b.text = "%s\n%s费" % [entry.get("id", "?"), _cost_of(entry.get("id", ""))]
			b.custom_minimum_size = Vector2(92, 96)
			b.pressed.connect(_select_hand.bind(entry))
			hand_container.add_child(b)
	var opp_count_v: Variant = state["seats"][1 - my_seat]["hand"]
	var opp_count: int = opp_count_v.get("count", 0) if typeof(opp_count_v) == TYPE_DICTIONARY else 0
	$Layout/Battle/Center/Hand/OppCount.text = "对手手牌: %d (内容不可见)" % opp_count

	var is_my_turn: bool = int(state.get("active", -1)) == my_seat and state.get("phase") == "main"
	end_button.disabled = not is_my_turn
	action_button.disabled = selected_hand == null
	var my_energy: int = int(state["seats"][my_seat].get("energy", 0))
	$Layout/Battle/Center/Hand/Energy.text = "能量: %d" % my_energy

func _cost_of(id: String) -> int:
	match id:
		"militia", "reinforce", "repair": return 1
		"infantry", "fireball", "counter", "banner", "poison": return 2
		"vanguard": return 3
		"guardian": return 4
	return 0

func _current_match_id() -> String:
	return str(state.get("match_id", "")) if state != null else ""

func _log(line: String) -> void:
	log_view.append_text(line + "\n")

# ------------------------------------------------------------- 令牌本地存储

func _config_path() -> String:
	return "user://tokens.cfg"

func _save_token(player: String, t: String) -> void:
	var cf := ConfigFile.new()
	cf.load(_config_path())
	cf.set_value("auth", "token_" + player, t)
	cf.save(_config_path())

func _load_token(player: String) -> String:
	var cf := ConfigFile.new()
	if cf.load(_config_path()) != OK:
		return ""
	return str(cf.get_value("auth", "token_" + player, ""))
