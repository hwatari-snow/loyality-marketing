import csv
import random
import datetime
from collections import defaultdict

random.seed(123)

TARGET_ROWS = 100000
REFERENCE_DATE = datetime.date(2026, 3, 15)

# ---------------------------------------------------------------------------
# 顧客マスタ読み込み
# ---------------------------------------------------------------------------
customers = []
with open("csv/customer_master.csv", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["membership_status"] != "退会":
            customers.append(row)

active_customers = [c for c in customers if c["membership_status"] == "アクティブ"]
dormant_customers = [c for c in customers if c["membership_status"] == "休眠"]

# ---------------------------------------------------------------------------
# チャネル × 提携先 定義
# ---------------------------------------------------------------------------
PARTNERS = {
    "実店舗": {
        "ローソン": {
            "weight": 30,
            "store_prefix": "LS",
            "store_count": 500,
            "categories": {
                "おにぎり・弁当": [
                    ("おにぎり 鮭", 140), ("おにぎり ツナマヨ", 130), ("おにぎり 明太子", 140),
                    ("幕の内弁当", 498), ("からあげ弁当", 450), ("のり弁当", 398),
                    ("サンドイッチ ミックス", 298), ("パスタ ナポリタン", 430),
                ],
                "飲料": [
                    ("マチカフェ コーヒーS", 110), ("マチカフェ コーヒーM", 150),
                    ("マチカフェ カフェラテ", 180), ("ペットボトル お茶 500ml", 160),
                    ("ペットボトル 水 500ml", 110), ("缶コーヒー", 130),
                    ("エナジードリンク", 220), ("炭酸飲料 500ml", 170),
                ],
                "菓子・スイーツ": [
                    ("プレミアムロールケーキ", 260), ("シュークリーム", 180),
                    ("チョコレート", 198), ("ポテトチップス", 170),
                    ("グミ", 148), ("アイスクリーム", 250),
                ],
                "日用品": [
                    ("ティッシュペーパー", 198), ("ハンドソープ", 298),
                    ("歯ブラシ", 248), ("マスク 7枚入", 398),
                    ("電池 単3 4本", 498), ("ウェットティッシュ", 198),
                ],
                "からあげクン": [
                    ("からあげクン レギュラー", 238), ("からあげクン レッド", 238),
                    ("からあげクン 北海道チーズ", 258),
                ],
                "たばこ・酒": [
                    ("ビール 350ml", 228), ("チューハイ 350ml", 168),
                    ("日本酒 カップ", 298), ("ワイン ミニボトル", 598),
                ],
            },
            "items_per_receipt": (1, 6),
            "point_rate": 0.005,
        },
        "ライフ": {
            "weight": 12,
            "store_prefix": "LF",
            "store_count": 300,
            "categories": {
                "生鮮食品": [
                    ("豚バラ肉 200g", 398), ("鶏もも肉 300g", 358), ("牛肉切り落とし 200g", 598),
                    ("サーモン刺身", 498), ("まぐろ刺身", 598), ("あじ開き 2枚", 298),
                    ("キャベツ 1玉", 168), ("トマト 3個", 298), ("きゅうり 3本", 198),
                    ("バナナ 1房", 158), ("りんご 3個", 398), ("みかん 1袋", 398),
                ],
                "加工食品": [
                    ("食パン 6枚切", 178), ("牛乳 1L", 228), ("卵 10個入", 258),
                    ("納豆 3パック", 108), ("豆腐 1丁", 88), ("ヨーグルト 400g", 178),
                    ("カレールー", 248), ("パスタ 500g", 198), ("ケチャップ", 218),
                ],
                "飲料": [
                    ("麦茶 2L", 158), ("ミネラルウォーター 2L", 108),
                    ("オレンジジュース 1L", 198), ("炭酸水 500ml×6", 498),
                ],
                "日用品": [
                    ("洗濯洗剤", 398), ("食器用洗剤", 198), ("トイレットペーパー 12R", 498),
                    ("キッチンペーパー", 198), ("ゴミ袋 30L 20枚", 298),
                ],
            },
            "items_per_receipt": (2, 10),
            "point_rate": 0.005,
        },
        "出光/シェル": {
            "weight": 8,
            "store_prefix": "GS",
            "store_count": 400,
            "categories": {
                "ガソリン": [
                    ("レギュラーガソリン 20L", 3400), ("レギュラーガソリン 30L", 5100),
                    ("レギュラーガソリン 40L", 6800), ("ハイオクガソリン 20L", 3600),
                    ("ハイオクガソリン 30L", 5400), ("軽油 30L", 4200),
                ],
                "カー用品": [
                    ("ウォッシャー液", 298), ("エンジンオイル交換", 3980),
                    ("タイヤ空気圧チェック", 0), ("洗車 水洗い", 500), ("洗車 撥水コート", 1500),
                ],
            },
            "items_per_receipt": (1, 2),
            "point_rate": 0.005,
        },
        "ケンタッキーフライドチキン": {
            "weight": 6,
            "store_prefix": "KF",
            "store_count": 200,
            "categories": {
                "チキン": [
                    ("オリジナルチキン", 310), ("オリジナルチキン 3P", 870),
                    ("骨なしケンタッキー", 290), ("レッドホットチキン", 320),
                    ("チキンナゲット 5P", 390),
                ],
                "セット": [
                    ("オリジナルチキンセット", 790), ("チキンフィレバーガーセット", 830),
                    ("和風チキンカツバーガーセット", 830), ("ツイスターセット", 780),
                ],
                "サイド・ドリンク": [
                    ("コールスロー S", 260), ("ビスケット", 270),
                    ("フライドポテト S", 280), ("ドリンク M", 280),
                ],
            },
            "items_per_receipt": (1, 4),
            "point_rate": 0.005,
        },
        "すき家": {
            "weight": 5,
            "store_prefix": "SK",
            "store_count": 250,
            "categories": {
                "牛丼": [
                    ("牛丼 並盛", 430), ("牛丼 大盛", 580), ("牛丼 特盛", 730),
                    ("ねぎ玉牛丼 並盛", 530), ("キムチ牛丼 並盛", 530),
                    ("おんたま牛丼 並盛", 510),
                ],
                "カレー": [
                    ("カレー 並盛", 530), ("チーズカレー 並盛", 630),
                ],
                "サイド": [
                    ("みそ汁", 90), ("おしんこ", 100), ("サラダ", 180),
                    ("たまごセット", 120),
                ],
            },
            "items_per_receipt": (1, 3),
            "point_rate": 0.005,
        },
        "AOKI": {
            "weight": 2,
            "store_prefix": "AK",
            "store_count": 100,
            "categories": {
                "スーツ": [
                    ("ビジネススーツ 2ピース", 29800), ("リクルートスーツ", 19800),
                    ("礼服", 39800),
                ],
                "シャツ": [
                    ("ワイシャツ レギュラー", 3980), ("ワイシャツ 形態安定", 4980),
                    ("ポロシャツ", 2980),
                ],
                "ネクタイ・小物": [
                    ("ネクタイ シルク", 3980), ("ベルト 牛革", 4980),
                    ("靴下 3足セット", 1480),
                ],
            },
            "items_per_receipt": (1, 3),
            "point_rate": 0.01,
        },
        "GEO": {
            "weight": 3,
            "store_prefix": "GE",
            "store_count": 150,
            "categories": {
                "ゲーム": [
                    ("中古ゲームソフト PS5", 3980), ("中古ゲームソフト Switch", 3480),
                    ("新品ゲームソフト", 6980), ("ゲームコントローラー", 4980),
                ],
                "DVD/Blu-ray": [
                    ("DVDレンタル 旧作", 110), ("DVDレンタル 新作", 390),
                    ("Blu-rayレンタル 旧作", 220), ("Blu-rayレンタル 新作", 490),
                ],
                "トレカ・ホビー": [
                    ("トレーディングカード パック", 198), ("フィギュア", 2980),
                    ("プラモデル", 1980),
                ],
            },
            "items_per_receipt": (1, 3),
            "point_rate": 0.01,
        },
        "ビックカメラ": {
            "weight": 2,
            "store_prefix": "BC",
            "store_count": 50,
            "categories": {
                "家電": [
                    ("ドライヤー", 4980), ("電気シェーバー", 8980),
                    ("加湿器", 6980), ("空気清浄機", 19800),
                    ("電子レンジ", 15800), ("炊飯器", 12800),
                ],
                "PC・スマホ周辺": [
                    ("USBケーブル", 980), ("モバイルバッテリー", 2980),
                    ("ワイヤレスイヤホン", 5980), ("マウス", 1980),
                    ("キーボード", 3980), ("SDカード 64GB", 1480),
                ],
                "消耗品": [
                    ("プリンタインク", 3480), ("電池 単3 8本", 698),
                    ("蛍光灯 LED", 1480),
                ],
            },
            "items_per_receipt": (1, 3),
            "point_rate": 0.01,
        },
        "高島屋": {
            "weight": 1,
            "store_prefix": "TK",
            "store_count": 20,
            "categories": {
                "食品・惣菜": [
                    ("銘菓 詰め合わせ", 3240), ("洋菓子 ギフト", 2700),
                    ("デパ地下 惣菜", 864), ("フルーツ ギフト", 5400),
                ],
                "ファッション": [
                    ("婦人服 ブラウス", 12800), ("紳士 ネクタイ", 8800),
                    ("バッグ", 25800), ("財布", 18800),
                ],
                "化粧品": [
                    ("化粧水", 5500), ("美容液", 8800), ("口紅", 3960),
                    ("ファンデーション", 4400),
                ],
            },
            "items_per_receipt": (1, 3),
            "point_rate": 0.005,
        },
        "ピザハット": {
            "weight": 2,
            "store_prefix": "PH",
            "store_count": 150,
            "categories": {
                "ピザ": [
                    ("マルゲリータ M", 1980), ("シーフード M", 2480),
                    ("テリヤキチキン M", 2180), ("プルコギ M", 2380),
                    ("ミックスピザ L", 2980), ("クワトロ L", 3280),
                ],
                "サイド": [
                    ("フライドポテト", 390), ("チキンナゲット", 490),
                    ("シーザーサラダ", 490),
                ],
                "ドリンク": [
                    ("コーラ 1.5L", 350), ("ジンジャーエール 1.5L", 350),
                ],
            },
            "items_per_receipt": (1, 4),
            "point_rate": 0.005,
        },
    },
    "ネットサービス": {
        "じゃらんnet": {
            "weight": 4,
            "categories": {
                "宿泊": [
                    ("ビジネスホテル 1泊", 6800), ("シティホテル 1泊", 12800),
                    ("温泉旅館 1泊2食", 18800), ("リゾートホテル 1泊", 25800),
                    ("ペンション 1泊2食", 9800), ("民宿 1泊", 5800),
                ],
                "レンタカー": [
                    ("レンタカー 軽 1日", 4980), ("レンタカー コンパクト 1日", 5980),
                    ("レンタカー ワゴン 1日", 9800),
                ],
            },
            "items_per_receipt": (1, 1),
            "point_rate": 0.02,
        },
        "ホットペッパービューティー": {
            "weight": 4,
            "categories": {
                "ヘアサロン": [
                    ("カット", 4400), ("カット+カラー", 8800), ("カット+パーマ", 10800),
                    ("トリートメント", 3300), ("ヘッドスパ", 4400),
                ],
                "ネイル・エステ": [
                    ("ジェルネイル", 5500), ("まつげエクステ", 4800),
                    ("フェイシャルエステ", 8800),
                ],
            },
            "items_per_receipt": (1, 2),
            "point_rate": 0.02,
        },
        "ホットペッパーグルメ": {
            "weight": 3,
            "categories": {
                "ネット予約": [
                    ("居酒屋 コース 1名", 3500), ("居酒屋 コース 1名", 4500),
                    ("イタリアン コース 1名", 5000), ("焼肉 コース 1名", 5500),
                    ("中華 コース 1名", 4000), ("和食 コース 1名", 6000),
                ],
            },
            "items_per_receipt": (1, 1),
            "point_rate": 0.02,
        },
        "au PAY マーケット": {
            "weight": 5,
            "categories": {
                "食品・飲料": [
                    ("米 5kg", 2480), ("ミネラルウォーター 24本", 1980),
                    ("コーヒー豆 500g", 1480), ("お取り寄せスイーツ", 2980),
                ],
                "日用品": [
                    ("洗剤 詰替 3個セット", 1280), ("シャンプー 詰替", 698),
                    ("サプリメント", 1980), ("マスク 50枚入", 698),
                ],
                "家電・雑貨": [
                    ("ワイヤレス充電器", 2480), ("ポータブル扇風機", 1980),
                    ("アロマディフューザー", 3480), ("スマホケース", 1480),
                ],
            },
            "items_per_receipt": (1, 4),
            "point_rate": 0.01,
        },
        "Pontaパス": {
            "weight": 2,
            "categories": {
                "サブスクリプション": [
                    ("Pontaパス 月額", 548),
                ],
            },
            "items_per_receipt": (1, 1),
            "point_rate": 0.01,
        },
    },
    "インフラ": {
        "au (携帯料金)": {
            "weight": 5,
            "categories": {
                "携帯料金": [
                    ("使い放題MAX 月額", 7238), ("スマホミニプラン 月額", 3465),
                    ("povo 月額", 2700), ("UQ mobile 月額", 2178),
                ],
            },
            "items_per_receipt": (1, 1),
            "point_rate": 0.01,
        },
        "auでんき": {
            "weight": 3,
            "categories": {
                "電気料金": [
                    ("電気料金 月額", None),  # special: random between 4000-15000
                ],
            },
            "items_per_receipt": (1, 1),
            "point_rate": 0.01,
        },
        "auガス": {
            "weight": 2,
            "categories": {
                "ガス料金": [
                    ("ガス料金 月額", None),  # special: random between 3000-12000
                ],
            },
            "items_per_receipt": (1, 1),
            "point_rate": 0.005,
        },
    },
    "ポイ活": {
        "Pontaリサーチ": {
            "weight": 4,
            "categories": {
                "アンケート": [
                    ("デイリーアンケート", None),  # 1-5 points
                    ("週間アンケート", None),  # 5-20 points
                    ("長期アンケート", None),  # 20-100 points
                ],
            },
            "items_per_receipt": (1, 1),
            "point_rate": None,  # direct points
        },
        "スキマ時間": {
            "weight": 3,
            "categories": {
                "ミニタスク": [
                    ("動画視聴", None),  # 1-5 points
                    ("アプリダウンロード", None),  # 50-200 points
                    ("ゲームプレイ", None),  # 1-10 points
                    ("レシート送信", None),  # 1-5 points
                ],
            },
            "items_per_receipt": (1, 1),
            "point_rate": None,
        },
        "Pontaカード提示キャンペーン": {
            "weight": 2,
            "categories": {
                "キャンペーン": [
                    ("来店ボーナスポイント", None),  # 5-50 points
                    ("期間限定ボーナス", None),  # 10-100 points
                    ("スタンプラリー達成", None),  # 50-500 points
                ],
            },
            "items_per_receipt": (1, 1),
            "point_rate": None,
        },
    },
}

# ---------------------------------------------------------------------------
# チャネル別の重み (ローソンが圧倒的に多い)
# ---------------------------------------------------------------------------
CHANNEL_TYPE_WEIGHTS = {"実店舗": 55, "ネットサービス": 20, "インフラ": 15, "ポイ活": 10}

PAYMENT_METHODS_STORE = ["現金", "現金", "au PAY", "au PAY", "au PAY",
                         "クレジットカード", "クレジットカード", "QRコード決済", "電子マネー"]
PAYMENT_METHODS_ONLINE = ["クレジットカード", "クレジットカード", "au PAY",
                          "au PAY", "ポイント全額", "auかんたん決済"]
PAYMENT_METHODS_INFRA = ["口座振替", "口座振替", "クレジットカード", "au PAY"]

# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------
def random_time():
    hour = random.choices(
        range(24),
        weights=[1,0,0,0,0,1,3,8,10,8,7,9,12,9,8,9,10,11,12,10,8,6,4,2],
    )[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{hour:02d}:{minute:02d}:{second:02d}"

def pick_partner(channel_type):
    partners = PARTNERS[channel_type]
    names = list(partners.keys())
    weights = [partners[n]["weight"] for n in names]
    return random.choices(names, weights=weights)[0]

def pick_channel():
    types = list(CHANNEL_TYPE_WEIGHTS.keys())
    weights = [CHANNEL_TYPE_WEIGHTS[t] for t in types]
    return random.choices(types, weights=weights)[0]

def generate_transaction_date(customer):
    enroll = datetime.datetime.strptime(customer["enrollment_date"], "%Y-%m-%d").date()
    last_p = datetime.datetime.strptime(customer["last_purchase_date"], "%Y-%m-%d").date()
    end = min(last_p, REFERENCE_DATE - datetime.timedelta(days=1))
    if end < enroll:
        end = enroll
    delta = (end - enroll).days
    if delta <= 0:
        return enroll
    return enroll + datetime.timedelta(days=random.randint(0, delta))

# ---------------------------------------------------------------------------
# メイン生成ロジック
# ---------------------------------------------------------------------------
rows = []
transaction_counter = 0
receipt_counter = 0

customer_weights = []
for c in customers:
    rank = c["membership_rank"]
    if rank == "プラチナ":
        w = 8
    elif rank == "ゴールド":
        w = 5
    elif rank == "シルバー":
        w = 3
    else:
        w = 1
    if c["membership_status"] == "休眠":
        w *= 0.4
    customer_weights.append(w)

while len(rows) < TARGET_ROWS:
    customer = random.choices(customers, weights=customer_weights)[0]
    channel_type = pick_channel()
    partner_name = pick_partner(channel_type)
    partner = PARTNERS[channel_type][partner_name]

    txn_date = generate_transaction_date(customer)
    txn_time = random_time()

    receipt_counter += 1
    receipt_id = f"RCP{receipt_counter:08d}"

    store_id = ""
    store_name = ""
    if channel_type == "実店舗":
        prefix = partner["store_prefix"]
        num = random.randint(1, partner["store_count"])
        store_id = f"{prefix}{num:04d}"
        store_name = f"{partner_name} {customer['prefecture']}{customer['city']}店"

    min_items, max_items = partner["items_per_receipt"]
    num_items = random.randint(min_items, max_items)
    if len(rows) + num_items > TARGET_ROWS + 50:
        num_items = max(1, TARGET_ROWS - len(rows))

    cats = list(partner["categories"].keys())
    cat_weights = [max(1, len(partner["categories"][c])) for c in cats]

    points_used_receipt = 0
    if channel_type in ("実店舗", "ネットサービス") and random.random() < 0.12:
        points_used_receipt = random.choice([10, 20, 50, 100, 200, 500, 1000])

    if channel_type == "実店舗":
        payment = random.choice(PAYMENT_METHODS_STORE)
    elif channel_type == "ネットサービス":
        payment = random.choice(PAYMENT_METHODS_ONLINE)
    elif channel_type == "インフラ":
        payment = random.choice(PAYMENT_METHODS_INFRA)
    else:
        payment = "なし"

    for item_idx in range(num_items):
        transaction_counter += 1
        txn_id = f"TXN{transaction_counter:08d}"

        category = random.choices(cats, weights=cat_weights)[0]
        products = partner["categories"][category]
        product_name, base_price = random.choice(products)

        quantity = 1
        if channel_type == "実店舗" and category not in ("ガソリン",):
            if random.random() < 0.15:
                quantity = random.randint(2, 3)

        # Special price handling
        if base_price is None:
            if channel_type == "ポイ活":
                if "アンケート" in category:
                    if "デイリー" in product_name:
                        earned = random.randint(1, 5)
                    elif "週間" in product_name:
                        earned = random.randint(5, 20)
                    else:
                        earned = random.randint(20, 100)
                elif "ミニタスク" in category:
                    if "アプリダウンロード" in product_name:
                        earned = random.randint(50, 200)
                    elif "ゲームプレイ" in product_name:
                        earned = random.randint(1, 10)
                    else:
                        earned = random.randint(1, 5)
                else:
                    if "スタンプ" in product_name:
                        earned = random.randint(50, 500)
                    elif "期間限定" in product_name:
                        earned = random.randint(10, 100)
                    else:
                        earned = random.randint(5, 50)
                unit_price = 0
                total_amount = 0
                points_earned = earned
                points_used = 0
                payment = "なし"
            elif partner_name == "auでんき":
                unit_price = random.randint(4000, 15000)
                total_amount = unit_price
                points_earned = int(total_amount * 0.01)
                points_used = 0
            elif partner_name == "auガス":
                unit_price = random.randint(3000, 12000)
                total_amount = unit_price
                points_earned = int(total_amount * 0.005)
                points_used = 0
            else:
                unit_price = 0
                total_amount = 0
                points_earned = random.randint(1, 10)
                points_used = 0
        else:
            unit_price = base_price
            total_amount = unit_price * quantity
            points_earned = max(1, int(total_amount * partner["point_rate"]))
            points_used = points_used_receipt if item_idx == 0 else 0

        rows.append([
            txn_id,
            receipt_id,
            customer["customer_id"],
            txn_date.strftime("%Y-%m-%d"),
            txn_time,
            channel_type,
            partner_name,
            store_id,
            store_name,
            category,
            product_name,
            quantity,
            unit_price,
            total_amount,
            points_earned,
            points_used,
            payment,
        ])

    if len(rows) >= TARGET_ROWS:
        break

rows = rows[:TARGET_ROWS]

HEADERS = [
    "transaction_id",
    "receipt_id",
    "customer_id",
    "transaction_date",
    "transaction_time",
    "channel_type",
    "partner_name",
    "store_id",
    "store_name",
    "product_category",
    "product_name",
    "quantity",
    "unit_price",
    "total_amount",
    "points_earned",
    "points_used",
    "payment_method",
]

output_path = "csv/transactions.csv"
with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(HEADERS)
    writer.writerows(rows)

print(f"Generated {len(rows)} transaction rows -> {output_path}")

# summary
from collections import Counter
ch = Counter(r[5] for r in rows)
print("\n【チャネル別件数】")
for k, v in ch.most_common():
    print(f"  {k}: {v:,} ({v/len(rows)*100:.1f}%)")

pa = Counter(r[6] for r in rows)
print("\n【提携先 TOP10】")
for k, v in pa.most_common(10):
    print(f"  {k}: {v:,} ({v/len(rows)*100:.1f}%)")

print(f"\nユニーク顧客数: {len(set(r[2] for r in rows)):,}")
print(f"ユニークレシート数: {len(set(r[1] for r in rows)):,}")
