"""运行配置：端口、跨域、运行环境。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    app_name: str = "管道工坊 · 实时数仓与流式管道平台"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 窗口运行时：迟到订单的默认处理策略，必须是“立即更正”或“延迟合并”。
    window_late_policy: str = os.getenv("WINDOW_LATE_POLICY", "立即更正")
    # 桶/批次查询单页上限，避免一次拉全量。
    window_page_size_max: int = 500


settings = Settings()
