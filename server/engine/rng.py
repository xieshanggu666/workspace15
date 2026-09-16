"""确定性 RNG 与洗牌。

回放时服务端按序重放洗牌事件(含种子), 因此不依赖 Python 内置 hash;
直接用显式种子的 SplitMix64 风格 LCG, 跨版本/跨平台结果一致。
"""

from __future__ import annotations

MASK = (1 << 64) - 1
# Knuth MMIX LCG 乘子
MUL = 6364136223846793005
INC = 1442695040888963407


class DeterministicRNG:
    def __init__(self, seed: int):
        self.state = seed & MASK

    def next_u64(self) -> int:
        self.state = (self.state * MUL + INC) & MASK
        # xorshift-mix
        z = self.state
        z ^= z >> 30
        z = (z * 0xBF58476D1CE4E5B9) & MASK
        z ^= z >> 27
        z = (z * 0x94D049BB133111EB) & MASK
        z ^= z >> 31
        return z

    def randbelow(self, n: int) -> int:
        if n <= 0:
            raise ValueError("n must be positive")
        return self.next_u64() % n


def shuffled(items: list, seed: int) -> list:
    """Fisher-Yates 洗牌, 纯函数(不修改输入)。"""
    result = list(items)
    rng = DeterministicRNG(seed)
    for i in range(len(result) - 1, 0, -1):
        j = rng.randbelow(i + 1)
        result[i], result[j] = result[j], result[i]
    return result
