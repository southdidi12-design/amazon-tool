import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from amazon_tool.ml_autopilot import train_and_save_model


def main():
    result = train_and_save_model(days=180)
    print(
        f"模型训练完成: path={result.model_path}, samples={result.rows}, campaigns={result.campaigns}, mae={result.mae:.4f}"
    )


if __name__ == "__main__":
    main()
