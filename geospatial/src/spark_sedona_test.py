from sedona.spark import SedonaContext


def main():
    config = (
        SedonaContext.builder()
        .appName("GeoPulse-Spark-Sedona-Test")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-4.0_2.13:1.9.1,"
            "org.datasyslab:geotools-wrapper:1.9.1-33.5"
        )
        .config(
            "spark.jars.repositories",
            "https://artifacts.unidata.ucar.edu/repository/unidata-all"
        )
        .getOrCreate()
    )

    sedona = SedonaContext.create(config)

    print("Spark version:", config.version)
    print("Apache Sedona: initialized successfully")

    config.stop()


if __name__ == "__main__":
    main()