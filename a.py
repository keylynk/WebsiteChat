from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date, when, length, expr


def parse_view_count(view_count_str):
    suffixes = {"B": 1e9, "M": 1e6}
    suffix = view_count_str[-1]
    if suffix in suffixes:
        multiplier = suffixes[suffix]
        return float(view_count_str[:-1]) * multiplier
    else:
        return float(view_count_str)


def main():
    print("Câu 1:")
    spark = SparkSession.builder.appName("Read CSV with DataFrame").getOrCreate()

    df = spark.read.csv("detai9/datatiktok.csv", header=True)

    # Chuyển đổi định dạng của cột thời gian tham gia thành kiểu ngày tháng
    df = df.withColumn("join_date", to_date(split(df["Account"], " ")[1], "MM/dd/yyyy"))

    # Tìm tiktoker có thời gian tham gia sớm nhất
    earliest_join_date = df.orderBy("join_date").first()  # last()
    print(
        "Tiktoker có thời gian tham gia sớm nhất là:",
        earliest_join_date["Name"],
        "và tham gia vào ngày:",
        earliest_join_date["join_date"],
    )

    print("Câu 2:")
    # Tìm tiktoker có số lượng fan nhiều nhất
    most_fans_tiktoker = df.orderBy(col("Fans").cast("float").desc()).first()  # asc()
    print(
        "Tiktoker có số lượng fan nhiều nhất là:",
        most_fans_tiktoker["Name"],
        "với số lượng fan là:",
        most_fans_tiktoker["Fans"],
    )

    print("Câu 3:")
    # Trích xuất năm gia nhập của mỗi tiktoker
    df = df.withColumn("join_year", split(df["join_date"], "-").getItem(0))
    join_years = df.select("join_year").distinct().collect()
    join_years = [row.join_year for row in join_years]

    print("Các năm gia nhập của các tiktoker là:")
    for year in join_years:
        print(year)

    print("Câu 4:")
    # Lấy 3 tiktoker hàng đầu có số lượng fan cao nhất
    top_tiktokers = (
        df.orderBy(col("Fans").cast("float").desc()).limit(3).collect()
    )  # asc()
    print("Danh sách 3 tiktoker hàng đầu có số lượng fan cao nhất:")
    for tiktoker in top_tiktokers:
        print("Tên:", tiktoker["Name"], "- Số lượng fan:", tiktoker["Fans"])

    print("Câu 5:")
    # Lấy 3 tiktoker hàng đầu theo tổng lượng fan và số lượng view
    df = df.withColumn(
        "total_score", col("Fans").cast("float") + col("Views").cast("float")
    )
    top_tiktokers_score = df.orderBy(col("total_score").desc()).limit(3).collect()
    print("Danh sách 3 tiktoker hàng đầu theo tổng lượng fan và số lượng view:")
    for tiktoker in top_tiktokers_score:
        print(
            "Tên:",
            tiktoker["Name"],
            "- Tổng lượng fan và số lượng view:",
            tiktoker["total_score"],
        )

    print("Câu 6:")
    # Lấy 3 tiktoker có số lượng fan thấp nhất và tính tỉ lệ giữa số lượng fan và số lượng view
    bottom_tiktokers = df.orderBy(col("Fans").cast("float")).limit(3).collect()
    ratios = []
    for tiktoker in bottom_tiktokers:
        fan_count = float(tiktoker["Fans"][:-1])
        view_count = float(tiktoker["Views"][:-1])
        ratio = fan_count / view_count if view_count != 0 else 0
        ratios.append((tiktoker["Name"], ratio))
    print(
        "Tỷ lệ giữa số lượng fan và số lượng view của 3 tiktoker có số lượng fan thấp nhất:"
    )
    for tiktoker, ratio in ratios:
        print("Tên:", tiktoker, "- Tỷ lệ:", ratio)

    print("Câu 7:")
    # Tính tỷ lệ giữa số lượng video và số lượng view của 3 tiktoker có số lượng view cao nhất
    top_tiktokers_view = (
        df.orderBy(col("Views").cast("float").desc()).limit(3).collect()
    )
    ratios = []
    for tiktoker in top_tiktokers_view:
        video_count = float(tiktoker["Videos"])
        view_count = parse_view_count(tiktoker["Views"])
        ratio = view_count / video_count if video_count != 0 else 0
        ratios.append((tiktoker["Name"], ratio))
    print(
        "Tỷ lệ giữa số lượng video và số lượng view của 3 tiktoker có số lượng view cao nhất:"
    )
    for tiktoker, ratio in ratios:
        print("Tên:", tiktoker, "- Tỷ lệ:", ratio)

    print("Câu 8:")
    # Apply the parsing logic directly within the DataFrame transformation
    df = df.withColumn(
        "Views_parsed",
        when(
            df["Views"].endswith("B"),
            expr("cast(trim(trailing 'B' from Views) as float) * 1e9"),
        )
        .when(
            df["Views"].endswith("M"),
            expr("cast(trim(trailing 'M' from Views) as float) * 1e6"),
        )
        .otherwise(col("Views").cast("float")),
    )  # tính lượng view theo từng tiktoker ( nhân với tỉ lệ )

    # Calculate the average view count per TikToker
    average_view_per_tiktoker = (
        df.select("Views_parsed").agg({"Views_parsed": "avg"}).collect()[0][0]
    )  # tính ra trung bình của mỗi tiktoker

    print("Số lượng view trung bình của mỗi tiktoker là:", average_view_per_tiktoker)

    print("Câu 9:")
    # Thêm cột mới với loại của số lượng video
    df = df.withColumn(
        "video_type",
        when(col("Videos") < 1000, "TYPE 1")
        .when((col("Videos") >= 1000) & (col("Videos") < 2000), "TYPE 2")
        .otherwise("TYPE 3"),
    )
    print("Thông tin của các tiktoker với cột mới:")
    df.show(truncate=False)

    print("Câu 10:")
    # Thêm cột mới với tỉ lệ giữa số lượng view, fan và video
    df = df.withColumn(
        "view_fan_video_ratio",
        col("Views").cast("float")
        / (col("Fans").cast("float") * col("Videos").cast("float")),
    )
    print("Thông tin của các tiktoker:")
    df.show(truncate=False)

    print("Câu 11:")
    df = df.withColumn(
        "Fans_M",
        when(
            df["Fans"].endswith("M"),
            expr("cast(trim(trailing 'M' from Fans) as float) * 1e6"),
        ).otherwise(col("Fans").cast("float")),
    )
    tiktokers_above_3M_fans = df.filter(col("Fans_M") > 3e6).count()
    print("Có tổng số tiktoker có số fan trên 3M là:", tiktokers_above_3M_fans)

    print("Câu 11:")
    df = df.withColumn(
        "Fans_M",
        when(
            df["Fans"].endswith("M"),
            expr("cast(trim(trailing 'M' from Fans) as float) * 1e6"),
        ).otherwise(col("Fans").cast("float")),
    )

    tiktokers_between_1M_and_3M_fans = (
        df.filter((col("Fans_M") >= 1e6) & (col("Fans_M") <= 3e6))
        .select("Name", "Fans")
        .collect()
    )

    print("Danh sách các tiktoker có số lượng fan từ 1M đến 3M:")
    for tiktoker in tiktokers_between_1M_and_3M_fans:
        print("Tên:", tiktoker["Name"], "- Số lượng fan:", tiktoker["Fans"])

    print("Câu 11:")
    df = df.withColumn(
        "Views_M",
        when(
            df["Views"].endswith("M"),
            expr("cast(trim(trailing 'M' from Views) as float) * 1e6"),
        )
        .when(
            df["Views"].endswith("B"),
            expr("cast(trim(trailing 'B' from Views) as float) * 1e9"),
        )
        .otherwise(col("Views").cast("float")),
    )

    tiktokers_above_10M_views_count = df.filter(col("Views_M") > 10e6).count()
    print(
        "Có tổng số tiktoker có lượng view trên 10M là:",
        tiktokers_above_10M_views_count,
    )

    print("Câu 11:")

    df = df.withColumn(
        "Fans_M",
        when(
            df["Fans"].endswith("M"),
            expr("cast(trim(trailing 'M' from Fans) as float) * 1e6"),
        ).otherwise(col("Fans").cast("float")),
    )

    tiktokers_below_2M_fans_count = df.filter(col("Fans_M") < 2e6).count()
    print(
        "Có tổng số tiktoker có lượng fan ít hơn 2M là:", tiktokers_below_2M_fans_count
    )


if __name__ == "__main__":
    main()

d
