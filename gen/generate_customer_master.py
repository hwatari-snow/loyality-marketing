import csv
import random
import datetime

random.seed(42)

NUM_RECORDS = 10000

PREFECTURES_CITIES = {
    "北海道": ["札幌市中央区", "札幌市北区", "札幌市東区", "旭川市", "函館市", "釧路市", "帯広市", "小樽市", "北見市", "苫小牧市"],
    "青森県": ["青森市", "八戸市", "弘前市", "十和田市", "むつ市"],
    "岩手県": ["盛岡市", "一関市", "奥州市", "花巻市", "北上市"],
    "宮城県": ["仙台市青葉区", "仙台市宮城野区", "仙台市太白区", "石巻市", "大崎市", "名取市"],
    "秋田県": ["秋田市", "横手市", "大仙市", "由利本荘市"],
    "山形県": ["山形市", "鶴岡市", "酒田市", "米沢市"],
    "福島県": ["福島市", "郡山市", "いわき市", "会津若松市"],
    "茨城県": ["水戸市", "つくば市", "日立市", "ひたちなか市", "土浦市"],
    "栃木県": ["宇都宮市", "小山市", "栃木市", "足利市", "佐野市"],
    "群馬県": ["前橋市", "高崎市", "太田市", "伊勢崎市", "桐生市"],
    "埼玉県": ["さいたま市大宮区", "さいたま市浦和区", "川越市", "川口市", "所沢市", "越谷市", "春日部市", "草加市", "上尾市", "熊谷市"],
    "千葉県": ["千葉市中央区", "千葉市美浜区", "船橋市", "松戸市", "市川市", "柏市", "市原市", "八千代市", "流山市", "浦安市"],
    "東京都": ["千代田区", "中央区", "港区", "新宿区", "渋谷区", "豊島区", "文京区", "台東区", "墨田区", "江東区", "品川区", "目黒区", "大田区", "世田谷区", "杉並区", "練馬区", "板橋区", "北区", "足立区", "葛飾区", "江戸川区", "八王子市", "町田市", "府中市", "調布市", "立川市", "武蔵野市", "三鷹市"],
    "神奈川県": ["横浜市西区", "横浜市中区", "横浜市港北区", "横浜市青葉区", "横浜市鶴見区", "川崎市川崎区", "川崎市中原区", "相模原市", "藤沢市", "横須賀市", "平塚市", "茅ヶ崎市", "厚木市", "鎌倉市"],
    "新潟県": ["新潟市中央区", "長岡市", "上越市", "三条市"],
    "富山県": ["富山市", "高岡市", "射水市"],
    "石川県": ["金沢市", "白山市", "小松市"],
    "福井県": ["福井市", "坂井市", "越前市"],
    "山梨県": ["甲府市", "甲斐市", "南アルプス市"],
    "長野県": ["長野市", "松本市", "上田市", "飯田市", "佐久市"],
    "岐阜県": ["岐阜市", "大垣市", "各務原市", "多治見市"],
    "静岡県": ["静岡市葵区", "浜松市中区", "沼津市", "富士市", "磐田市", "焼津市", "藤枝市"],
    "愛知県": ["名古屋市中区", "名古屋市中村区", "名古屋市東区", "名古屋市千種区", "名古屋市名東区", "名古屋市天白区", "名古屋市緑区", "豊田市", "岡崎市", "一宮市", "豊橋市", "春日井市", "安城市"],
    "三重県": ["津市", "四日市市", "鈴鹿市", "松阪市", "伊勢市"],
    "滋賀県": ["大津市", "草津市", "長浜市", "彦根市"],
    "京都府": ["京都市下京区", "京都市中京区", "京都市左京区", "京都市右京区", "京都市伏見区", "宇治市", "亀岡市", "舞鶴市"],
    "大阪府": ["大阪市北区", "大阪市中央区", "大阪市天王寺区", "大阪市浪速区", "大阪市西区", "大阪市淀川区", "大阪市城東区", "堺市堺区", "堺市中区", "豊中市", "吹田市", "高槻市", "枚方市", "東大阪市", "茨木市", "八尾市"],
    "兵庫県": ["神戸市中央区", "神戸市東灘区", "神戸市灘区", "神戸市兵庫区", "姫路市", "西宮市", "尼崎市", "明石市", "加古川市", "宝塚市"],
    "奈良県": ["奈良市", "橿原市", "生駒市", "大和郡山市"],
    "和歌山県": ["和歌山市", "田辺市", "橋本市"],
    "鳥取県": ["鳥取市", "米子市", "倉吉市"],
    "島根県": ["松江市", "出雲市", "浜田市"],
    "岡山県": ["岡山市北区", "倉敷市", "津山市", "総社市"],
    "広島県": ["広島市中区", "広島市南区", "広島市安佐南区", "福山市", "呉市", "東広島市", "尾道市"],
    "山口県": ["下関市", "山口市", "宇部市", "周南市"],
    "徳島県": ["徳島市", "阿南市", "鳴門市"],
    "香川県": ["高松市", "丸亀市", "坂出市"],
    "愛媛県": ["松山市", "今治市", "新居浜市", "西条市"],
    "高知県": ["高知市", "南国市", "四万十市"],
    "福岡県": ["福岡市博多区", "福岡市中央区", "福岡市早良区", "福岡市東区", "北九州市小倉北区", "北九州市八幡西区", "久留米市", "飯塚市", "大牟田市", "春日市"],
    "佐賀県": ["佐賀市", "唐津市", "鳥栖市"],
    "長崎県": ["長崎市", "佐世保市", "諫早市", "大村市"],
    "熊本県": ["熊本市中央区", "熊本市東区", "八代市", "天草市", "玉名市"],
    "大分県": ["大分市", "別府市", "中津市", "佐伯市"],
    "宮崎県": ["宮崎市", "都城市", "延岡市", "日向市"],
    "鹿児島県": ["鹿児島市", "霧島市", "鹿屋市", "薩摩川内市"],
    "沖縄県": ["那覇市", "浦添市", "宜野湾市", "沖縄市", "豊見城市", "うるま市", "名護市"],
}

PREFECTURE_WEIGHTS = {
    "東京都": 1400, "神奈川県": 920, "大阪府": 880, "愛知県": 750, "埼玉県": 730,
    "千葉県": 630, "兵庫県": 550, "北海道": 520, "福岡県": 510, "静岡県": 360,
    "茨城県": 290, "広島県": 280, "京都府": 260, "宮城県": 230, "新潟県": 220,
    "長野県": 200, "岐阜県": 200, "群馬県": 190, "栃木県": 190, "岡山県": 190,
    "三重県": 180, "熊本県": 170, "福島県": 180, "鹿児島県": 160, "沖縄県": 150,
    "滋賀県": 140, "奈良県": 130, "山口県": 130, "愛媛県": 130, "長崎県": 130,
    "青森県": 120, "岩手県": 120, "石川県": 110, "大分県": 110, "山形県": 100,
    "宮崎県": 100, "富山県": 100, "秋田県": 90, "香川県": 95, "和歌山県": 90,
    "佐賀県": 80, "山梨県": 80, "福井県": 75, "徳島県": 70, "高知県": 68,
    "島根県": 65, "鳥取県": 55,
}

LAST_NAMES = [
    "佐藤", "鈴木", "高橋", "田中", "伊藤", "渡辺", "山本", "中村", "小林", "加藤",
    "吉田", "山田", "佐々木", "松本", "井上", "木村", "林", "斎藤", "清水", "山崎",
    "森", "池田", "橋本", "阿部", "石川", "山下", "中島", "石井", "小川", "前田",
    "岡田", "長谷川", "藤田", "後藤", "近藤", "村上", "遠藤", "青木", "坂本", "斉藤",
    "福田", "太田", "西村", "藤井", "金子", "岡本", "藤原", "三浦", "中野", "原田",
    "松田", "竹内", "中山", "和田", "石田", "上田", "森田", "小島", "柴田", "原",
    "宮崎", "酒井", "工藤", "横山", "宮本", "内田", "高木", "安藤", "谷口", "大野",
    "丸山", "今井", "河野", "藤本", "村田", "武田", "上野", "杉山", "増田", "小野",
    "平野", "大塚", "千葉", "久保", "松井", "野口", "菊地", "木下", "野村", "新井",
    "渡部", "桜井", "菅原", "山口", "熊谷", "佐野", "小松", "望月", "星野", "松尾",
]

MALE_FIRST_NAMES = [
    "太郎", "一郎", "健太", "大輝", "翔太", "拓也", "和也", "達也", "健一", "雄太",
    "直樹", "大介", "浩二", "誠", "隆", "哲也", "秀樹", "正樹", "裕太", "康平",
    "亮太", "悠太", "陸", "蓮", "悠斗", "颯太", "大翔", "翔", "湊", "奏太",
    "修一", "賢一", "博之", "幸一", "慎一", "淳", "剛", "学", "豊", "実",
    "智也", "竜也", "将大", "海斗", "優太", "颯", "樹", "陽太", "大地", "瑛太",
]

FEMALE_FIRST_NAMES = [
    "花子", "由美", "美香", "恵子", "陽子", "裕子", "明美", "久美子", "真由美", "直美",
    "さくら", "美咲", "葵", "結衣", "陽菜", "凛", "楓", "莉子", "芽依", "心春",
    "洋子", "京子", "和子", "節子", "幸子", "康子", "典子", "弘子", "順子", "美智子",
    "愛", "舞", "遥", "彩", "萌", "真由", "千尋", "里奈", "麻衣", "沙織",
    "美優", "結菜", "杏", "紬", "咲良", "琴音", "日葵", "詩", "芽生", "彩花",
]

OCCUPATIONS = [
    "会社員", "会社員", "会社員", "会社員",
    "公務員", "自営業", "パート・アルバイト", "パート・アルバイト",
    "専業主婦・主夫", "学生", "会社役員", "医療従事者",
    "教育関係", "IT・エンジニア", "販売・サービス", "製造業",
    "金融・保険", "不動産", "運輸・物流", "農林水産業",
    "フリーランス", "無職・退職", "その他",
]

INCOME_RANGES = [
    "200万円未満", "200万〜400万円", "200万〜400万円", "200万〜400万円",
    "400万〜600万円", "400万〜600万円", "400万〜600万円",
    "600万〜800万円", "600万〜800万円",
    "800万〜1000万円", "1000万〜1500万円", "1500万円以上",
]

MEMBERSHIP_RANKS = ["レギュラー", "シルバー", "ゴールド", "プラチナ"]
RANK_WEIGHTS = [50, 30, 15, 5]

CHANNELS = ["店舗", "Web", "アプリ", "キャンペーン", "紹介"]
CHANNEL_WEIGHTS = [35, 25, 25, 10, 5]

MAIL_DOMAINS = [
    "gmail.com", "yahoo.co.jp", "icloud.com", "outlook.jp",
    "docomo.ne.jp", "ezweb.ne.jp", "softbank.ne.jp", "au.com",
]

def generate_email(last_name_roma, first_name_roma, customer_id):
    domain = random.choice(MAIL_DOMAINS)
    patterns = [
        f"{last_name_roma}.{first_name_roma}",
        f"{first_name_roma}.{last_name_roma}",
        f"{last_name_roma}{customer_id}",
        f"{first_name_roma}{random.randint(1, 999)}",
    ]
    local = random.choice(patterns)
    return f"{local}@{domain}"

ROMA_LAST = {
    "佐藤": "sato", "鈴木": "suzuki", "高橋": "takahashi", "田中": "tanaka",
    "伊藤": "ito", "渡辺": "watanabe", "山本": "yamamoto", "中村": "nakamura",
    "小林": "kobayashi", "加藤": "kato", "吉田": "yoshida", "山田": "yamada",
    "佐々木": "sasaki", "松本": "matsumoto", "井上": "inoue", "木村": "kimura",
    "林": "hayashi", "斎藤": "saito", "清水": "shimizu", "山崎": "yamazaki",
    "森": "mori", "池田": "ikeda", "橋本": "hashimoto", "阿部": "abe",
    "石川": "ishikawa", "山下": "yamashita", "中島": "nakajima", "石井": "ishii",
    "小川": "ogawa", "前田": "maeda", "岡田": "okada", "長谷川": "hasegawa",
    "藤田": "fujita", "後藤": "goto", "近藤": "kondo", "村上": "murakami",
    "遠藤": "endo", "青木": "aoki", "坂本": "sakamoto", "斉藤": "saito2",
    "福田": "fukuda", "太田": "ota", "西村": "nishimura", "藤井": "fujii",
    "金子": "kaneko", "岡本": "okamoto", "藤原": "fujiwara", "三浦": "miura",
    "中野": "nakano", "原田": "harada", "松田": "matsuda", "竹内": "takeuchi",
    "中山": "nakayama", "和田": "wada", "石田": "ishida", "上田": "ueda",
    "森田": "morita", "小島": "kojima", "柴田": "shibata", "原": "hara",
    "宮崎": "miyazaki", "酒井": "sakai", "工藤": "kudo", "横山": "yokoyama",
    "宮本": "miyamoto", "内田": "uchida", "高木": "takagi", "安藤": "ando",
    "谷口": "taniguchi", "大野": "ono", "丸山": "maruyama", "今井": "imai",
    "河野": "kawano", "藤本": "fujimoto", "村田": "murata", "武田": "takeda",
    "上野": "ueno", "杉山": "sugiyama", "増田": "masuda", "小野": "ono2",
    "平野": "hirano", "大塚": "otsuka", "千葉": "chiba", "久保": "kubo",
    "松井": "matsui", "野口": "noguchi", "菊地": "kikuchi", "木下": "kinoshita",
    "野村": "nomura", "新井": "arai", "渡部": "watabe", "桜井": "sakurai",
    "菅原": "sugawara", "山口": "yamaguchi", "熊谷": "kumagai", "佐野": "sano",
    "小松": "komatsu", "望月": "mochizuki", "星野": "hoshino", "松尾": "matsuo",
}

def generate_phone():
    prefix = random.choice(["070", "080", "090"])
    return f"{prefix}-{random.randint(1000,9999)}-{random.randint(1000,9999)}"

def random_date(start, end):
    delta = end - start
    rand_days = random.randint(0, delta.days)
    return start + datetime.timedelta(days=rand_days)

def generate_birth_date_from_age(age, reference_date):
    year = reference_date.year - age
    start = datetime.date(year, 1, 1)
    end = datetime.date(year, 12, 31)
    if end > reference_date:
        end = reference_date
    return random_date(start, end)

def age_weighted():
    r = random.random()
    if r < 0.05:
        return random.randint(18, 24)
    elif r < 0.20:
        return random.randint(25, 34)
    elif r < 0.45:
        return random.randint(35, 44)
    elif r < 0.70:
        return random.randint(45, 54)
    elif r < 0.88:
        return random.randint(55, 64)
    else:
        return random.randint(65, 85)

def points_by_rank(rank):
    if rank == "プラチナ":
        return random.randint(50000, 200000)
    elif rank == "ゴールド":
        return random.randint(15000, 60000)
    elif rank == "シルバー":
        return random.randint(3000, 20000)
    else:
        return random.randint(0, 5000)


REFERENCE_DATE = datetime.date(2026, 3, 15)
ENROLLMENT_START = datetime.date(2010, 4, 1)
ENROLLMENT_END = datetime.date(2026, 3, 14)

prefectures = list(PREFECTURE_WEIGHTS.keys())
pref_weights = [PREFECTURE_WEIGHTS[p] for p in prefectures]

rows = []
for i in range(1, NUM_RECORDS + 1):
    customer_id = f"PNT{i:07d}"

    gender = random.choices(["男性", "女性"], weights=[48, 52])[0]
    last_name = random.choice(LAST_NAMES)
    if gender == "男性":
        first_name = random.choice(MALE_FIRST_NAMES)
    else:
        first_name = random.choice(FEMALE_FIRST_NAMES)
    customer_name = f"{last_name} {first_name}"

    age = age_weighted()
    birth_date = generate_birth_date_from_age(age, REFERENCE_DATE)

    prefecture = random.choices(prefectures, weights=pref_weights)[0]
    city = random.choice(PREFECTURES_CITIES[prefecture])

    enrollment_date = random_date(ENROLLMENT_START, ENROLLMENT_END)
    rank = random.choices(MEMBERSHIP_RANKS, weights=RANK_WEIGHTS)[0]
    total_points = points_by_rank(rank)

    occupation = random.choice(OCCUPATIONS)
    income_range = random.choice(INCOME_RANGES)

    roma_last = ROMA_LAST.get(last_name, "user")
    email = generate_email(roma_last, f"u{i}", customer_id)
    phone = generate_phone()

    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]

    days_since_enroll = (REFERENCE_DATE - enrollment_date).days
    if days_since_enroll > 0:
        last_purchase = enrollment_date + datetime.timedelta(
            days=random.randint(0, days_since_enroll)
        )
    else:
        last_purchase = enrollment_date

    if rank == "プラチナ":
        status = "アクティブ"
    elif rank == "ゴールド":
        status = random.choices(["アクティブ", "休眠"], weights=[90, 10])[0]
    elif rank == "シルバー":
        status = random.choices(["アクティブ", "休眠"], weights=[75, 25])[0]
    else:
        status = random.choices(["アクティブ", "休眠", "退会"], weights=[55, 35, 10])[0]

    dm_flag = random.choices([1, 0], weights=[70, 30])[0]
    app_flag = random.choices([1, 0], weights=[45, 55])[0]

    rows.append([
        customer_id,
        customer_name,
        gender,
        age,
        birth_date.strftime("%Y-%m-%d"),
        prefecture,
        city,
        enrollment_date.strftime("%Y-%m-%d"),
        total_points,
        rank,
        occupation,
        income_range,
        email,
        phone,
        channel,
        last_purchase.strftime("%Y-%m-%d"),
        status,
        dm_flag,
        app_flag,
    ])

HEADERS = [
    "customer_id",
    "customer_name",
    "gender",
    "age",
    "birth_date",
    "prefecture",
    "city",
    "enrollment_date",
    "total_points",
    "membership_rank",
    "occupation",
    "income_range",
    "email",
    "phone_number",
    "enrollment_channel",
    "last_purchase_date",
    "membership_status",
    "dm_consent_flag",
    "app_usage_flag",
]

output_path = "csv/customer_master.csv"
with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(HEADERS)
    writer.writerows(rows)

print(f"Generated {len(rows)} records -> {output_path}")
