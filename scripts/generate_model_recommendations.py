import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from amazon_tool.ml_autopilot import save_recommendations_csv


def main():
    path = save_recommendations_csv()
    print(f"推荐结果已导出: {path}")


if __name__ == "__main__":
    main()
