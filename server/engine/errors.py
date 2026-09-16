"""引擎错误。decide() 在校验失败时抛 RuleError, 服务端转为 error 消息,
不写库、不扣任何资源。"""


class RuleError(Exception):
    def __init__(self, code: str, message: str | None = None):
        self.code = code
        super().__init__(message or code)
