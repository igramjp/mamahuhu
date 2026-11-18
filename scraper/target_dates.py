"""
ジョブの対象日をJSTの「時刻」から決める(日付の単純な today / tomorrow は使わない)。

GitHub Actions の cron は数時間遅れて発火することがあり(2026-11実測: 3〜8時間)、
18時JSTのジョブが0時を過ぎて走ると `date +%Y%m%d` / `date -d tomorrow` が
1日先にずれる。その結果、土曜の結果を取るはずのジョブが(まだ走っていない)
日曜を見に行き、前日オッズも1日先を取りに行っていた。

ここでは「今が何時か」から、遅延していても狙った開催日になるように決める。

  python scraper/target_dates.py results   # 結果取得の対象日(古い順、空白区切り)
  python scraper/target_dates.py forward   # 発走前オッズ/順方向予想の対象日
"""

import sys
from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))  # 日本は夏時間なし

# 最終レース(16:30前後)の結果が出揃う時刻。これ以降は当日ぶんを取りに行く
RESULTS_READY_HOUR = 17
# 1R発走(9:50前後)より前の時刻。これより前なら「次の開催日」は当日
FIRST_RACE_HOUR = 9


def jst_now(now=None):
    """JSTの現在時刻。now はテスト用(tz付きdatetime)。"""
    return (now or datetime.now(timezone.utc)).astimezone(JST)


def jst_today(now=None):
    return jst_now(now).date()


def result_dates(now=None):
    """結果取得の対象日(YYYYMMDD、古い順)。

    - 17時以降: [昨日, 今日]   … 定刻(18時)の実行。昨日は取りこぼしの拾い直し
    - 17時より前: [昨日]       … 前日18時のジョブが遅れて日付をまたいだ場合。
                                 今日のレースはまだ終わっていないので対象外
    取得済みの日は scrape.py 側がスキップするので、昨日を常に含めても重複しない。
    """
    n = jst_now(now)
    days = [n.date() - timedelta(days=1)]
    if n.hour >= RESULTS_READY_HOUR:
        days.append(n.date())
    return [d.strftime("%Y%m%d") for d in days]


def forward_date(now=None):
    """発走前オッズ・順方向予想の対象日(YYYYMMDD)= これから発走する次の日。

    - 9時以降: 明日  … 定刻(金土18時)の前日夜スナップショット
    - 9時より前: 今日 … 前日夜のジョブが0時を過ぎた場合、または当日朝の更新
    """
    n = jst_now(now)
    d = n.date() if n.hour < FIRST_RACE_HOUR else n.date() + timedelta(days=1)
    return d.strftime("%Y%m%d")



def preview_dates(now=None):
    """前日ポスト(X)の (投稿対象のレース日, バイアス分析の参照日)。

    対象日は forward_date と同じ(これから発走する次の日)。金曜20時のジョブが
    遅れて土曜未明に走っても、日曜ではなく土曜のレースを対象にする。
    参照日は対象日の曜日から決める(実行した曜日には依存しない):
      - 土曜のレース → 前週の日曜(6日前)。金曜は開催がないため
      - それ以外(日曜・祝日月曜) → 前日。前日夜に生成済みの分析を使う
    """
    target = datetime.strptime(forward_date(now), "%Y%m%d").date()
    src = target - timedelta(days=6 if target.weekday() == 5 else 1)
    return target.strftime("%Y%m%d"), src.strftime("%Y%m%d")


def main():
    kind = sys.argv[1] if len(sys.argv) > 1 else ""
    if kind == "results":
        print(" ".join(result_dates()))
    elif kind == "forward":
        print(forward_date())
    else:
        sys.exit("usage: target_dates.py results|forward")


if __name__ == "__main__":
    main()
