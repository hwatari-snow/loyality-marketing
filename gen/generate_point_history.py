import csv
import random
import datetime
from collections import defaultdict

random.seed(456)

REFERENCE_DATE = datetime.date(2026, 3, 15)

# ---------------------------------------------------------------------------
# 顧客マスタ読み込み
# ---------------------------------------------------------------------------
customers = {}
with open("csv/customer_master.csv", encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
        customers[row["customer_id"]] = row

# ---------------------------------------------------------------------------
# トランザクション読み込み
# ---------------------------------------------------------------------------
transactions = []
with open("csv/transactions.csv", encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
        transactions.append(row)

# ---------------------------------------------------------------------------
# 顧客ごとにイベントを収集 → 最後に時系列ソートして balance_after を計算
# ---------------------------------------------------------------------------
# event: (date_str, sort_priority, event_type, points, source_type,
#         source_detail, transaction_id, note)
# sort_priority: 付与=0, ボーナス=1, 調整=2, 利用=3, 失効=4
#   (同一日内で付与が先、失効が後になるように)
customer_events = defaultdict(list)

# ===========================================================================
# 1. 付与 (transactions の points_earned > 0)
# ===========================================================================
for txn in transactions:
    earned = int(txn["points_earned"])
    if earned > 0:
        customer_events[txn["customer_id"]].append((
            txn["transaction_date"],
            0,
            "付与",
            earned,
            txn["channel_type"],
            txn["partner_name"],
            txn["transaction_id"],
            "通常ポイント",
        ))

# ===========================================================================
# 2. 利用 (transactions の points_used > 0)
# ===========================================================================
for txn in transactions:
    used = int(txn["points_used"])
    if used > 0:
        customer_events[txn["customer_id"]].append((
            txn["transaction_date"],
            3,
            "利用",
            -used,
            txn["channel_type"],
            txn["partner_name"],
            txn["transaction_id"],
            "ポイント利用",
        ))

# ===========================================================================
# 3. ボーナス (~8,000件)
# ===========================================================================
BONUS_TYPES = [
    ("誕生月ボーナス", "キャンペーン", "Ponta", 50, 200),
    ("ランクアップボーナス", "キャンペーン", "Ponta", 100, 1000),
    ("入会ボーナス", "キャンペーン", "Ponta", 100, 500),
    ("来店スタンプ達成ボーナス", "実店舗", "ローソン", 50, 300),
    ("au PAY 連携ボーナス", "ネットサービス", "au PAY", 200, 500),
    ("友達紹介ボーナス", "キャンペーン", "Ponta", 100, 300),
    ("期間限定キャンペーン", "キャンペーン", "Ponta", 20, 500),
]

bonus_count = 0
for cid, cdata in customers.items():
    if cdata["membership_status"] == "退会":
        continue

    enroll_date = datetime.datetime.strptime(cdata["enrollment_date"], "%Y-%m-%d").date()
    last_date = datetime.datetime.strptime(cdata["last_purchase_date"], "%Y-%m-%d").date()
    end_date = min(last_date, REFERENCE_DATE - datetime.timedelta(days=1))
    if end_date < enroll_date:
        end_date = enroll_date

    rank = cdata["membership_rank"]
    if rank == "プラチナ":
        n_bonus = random.randint(2, 5)
    elif rank == "ゴールド":
        n_bonus = random.randint(1, 4)
    elif rank == "シルバー":
        n_bonus = random.randint(0, 2)
    else:
        n_bonus = random.choices([0, 1], weights=[60, 40])[0]

    birth_month = int(cdata["birth_date"].split("-")[1])
    has_birthday_bonus = False

    for _ in range(n_bonus):
        btype, src_type, src_detail, pt_min, pt_max = random.choice(BONUS_TYPES)
        pts = random.randint(pt_min, pt_max)

        if btype == "入会ボーナス":
            ev_date = enroll_date + datetime.timedelta(days=random.randint(0, 7))
            if ev_date > end_date:
                ev_date = enroll_date
        elif btype == "誕生月ボーナス" and not has_birthday_bonus:
            has_birthday_bonus = True
            for y in range(enroll_date.year, end_date.year + 1):
                try:
                    bd = datetime.date(y, birth_month, random.randint(1, 28))
                except ValueError:
                    bd = datetime.date(y, birth_month, 28)
                if enroll_date <= bd <= end_date and random.random() < 0.4:
                    customer_events[cid].append((
                        bd.strftime("%Y-%m-%d"), 1, "ボーナス", pts,
                        src_type, src_detail, "", "誕生月ボーナス",
                    ))
                    bonus_count += 1
            continue
        else:
            delta = (end_date - enroll_date).days
            if delta <= 0:
                ev_date = enroll_date
            else:
                ev_date = enroll_date + datetime.timedelta(days=random.randint(0, delta))

        customer_events[cid].append((
            ev_date.strftime("%Y-%m-%d"), 1, "ボーナス", pts,
            src_type, src_detail, "", btype,
        ))
        bonus_count += 1

print(f"ボーナスイベント: {bonus_count:,}")

# ===========================================================================
# 4. 失効 (~3,000件)
#    Pontaルール: 最終利用日から1年間ポイント有効。休眠顧客中心に失効発生。
# ===========================================================================
expire_count = 0
for cid, cdata in customers.items():
    status = cdata["membership_status"]
    rank = cdata["membership_rank"]

    if status == "休眠":
        prob = 0.35
    elif status == "退会":
        prob = 0.60
    elif rank == "レギュラー":
        prob = 0.08
    else:
        prob = 0.02

    if random.random() >= prob:
        continue

    last_date = datetime.datetime.strptime(cdata["last_purchase_date"], "%Y-%m-%d").date()

    n_expire = random.randint(1, 3)
    for _ in range(n_expire):
        expire_date = last_date + datetime.timedelta(days=random.randint(365, 400))
        if expire_date > REFERENCE_DATE:
            expire_date = REFERENCE_DATE - datetime.timedelta(days=random.randint(1, 60))
        expire_pts = random.choice([10, 20, 50, 100, 200, 300, 500, 800, 1000, 1500])

        customer_events[cid].append((
            expire_date.strftime("%Y-%m-%d"), 4, "失効", -expire_pts,
            "システム", "Ponta", "", "有効期限切れ失効",
        ))
        expire_count += 1

print(f"失効イベント: {expire_count:,}")

# ===========================================================================
# 5. 調整 (~1,000件) - 返品・取消によるポイント調整
# ===========================================================================
earned_txns = [t for t in transactions if int(t["points_earned"]) > 0 and t["channel_type"] in ("実店舗", "ネットサービス")]
adjust_sample = random.sample(earned_txns, min(1000, len(earned_txns)))
adjust_count = 0

for txn in adjust_sample:
    earned = int(txn["points_earned"])
    txn_date = datetime.datetime.strptime(txn["transaction_date"], "%Y-%m-%d").date()
    adjust_date = txn_date + datetime.timedelta(days=random.randint(1, 14))
    if adjust_date > REFERENCE_DATE:
        adjust_date = REFERENCE_DATE

    notes = random.choice(["返品によるポイント取消", "取引キャンセル", "金額訂正によるポイント調整"])
    if "訂正" in notes:
        adj_pts = -random.randint(1, max(1, earned // 2))
    else:
        adj_pts = -earned

    customer_events[txn["customer_id"]].append((
        adjust_date.strftime("%Y-%m-%d"), 2, "調整", adj_pts,
        txn["channel_type"], txn["partner_name"], txn["transaction_id"], notes,
    ))
    adjust_count += 1

print(f"調整イベント: {adjust_count:,}")

# ===========================================================================
# 全イベントを時系列ソートし、balance_after を計算してCSV出力
# ===========================================================================
rows = []
point_counter = 0

for cid in sorted(customer_events.keys()):
    events = sorted(customer_events[cid], key=lambda e: (e[0], e[1]))
    balance = 0

    for ev in events:
        date_str, _, event_type, points, src_type, src_detail, txn_id, note = ev
        balance += points
        if balance < 0:
            balance = 0

        point_counter += 1
        point_id = f"PT{point_counter:08d}"

        if event_type in ("付与", "ボーナス"):
            ev_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            exp_date = (ev_date + datetime.timedelta(days=365)).strftime("%Y-%m-%d")
        else:
            exp_date = ""

        rows.append([
            point_id,
            cid,
            date_str,
            event_type,
            points,
            balance,
            src_type,
            src_detail,
            txn_id,
            exp_date,
            note,
        ])

HEADERS = [
    "point_id",
    "customer_id",
    "event_date",
    "event_type",
    "points",
    "balance_after",
    "source_type",
    "source_detail",
    "transaction_id",
    "expiration_date",
    "note",
]

output_path = "csv/point_history.csv"
with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(HEADERS)
    writer.writerows(rows)

print(f"\nGenerated {len(rows):,} point history rows -> {output_path}")

# ---------------------------------------------------------------------------
# サマリー
# ---------------------------------------------------------------------------
from collections import Counter
ev_types = Counter(r[3] for r in rows)
print("\n【event_type別件数】")
for t in ["付与", "ボーナス", "利用", "失効", "調整"]:
    v = ev_types.get(t, 0)
    print(f"  {t}: {v:,} ({v/len(rows)*100:.1f}%)")

print(f"\nユニーク顧客数: {len(set(r[1] for r in rows)):,}")

balances = defaultdict(int)
for r in rows:
    balances[r[1]] = r[5]
vals = list(balances.values())
print(f"最終残高 - 平均: {sum(vals)/len(vals):,.0f}pt / 中央値: {sorted(vals)[len(vals)//2]:,}pt / 最大: {max(vals):,}pt")
