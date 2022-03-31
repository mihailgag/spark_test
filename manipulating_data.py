from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import avg
from download_files import DownloadFiles

spark = SparkSession.builder.appName("Excercise").getOrCreate()


class ManipulatingData(DownloadFiles):
    def get_movie_names(self):
        """reading movie names wih their ids"""

        self.movie_names = spark.read.csv(
            "title.basics.tsv.gz", header=True, sep=r"\t"
        ).select("tconst", "primaryTitle")

    def get_criteria_for_ranking_movies(self):
        """
        getting all movies with their ratings from file.
        filtering only movies with 50+ votes.
        new criteria column which follows the equation: numVotes / avgNumVotes * averageRating
        showing top 10 movies with highest criteria score
        """
        self.get_movie_names()
        df_ratings = spark.read.csv(
            "title.ratings.tsv.gz", header=True, sep=r"\t", inferSchema=True
        )
        df_ratings = df_ratings.where(df_ratings.numVotes > 50)
        self.top_ten_movies = (
            df_ratings.withColumn(
                "criteria",
                df_ratings["numVotes"]
                / df_ratings.select(avg("numVotes")).collect()[0][0]
                * df_ratings["averageRating"],
            )
            .orderBy("criteria", ascending=False)
            .limit(10)
        )
        self.top_ten_movies = self.top_ten_movies.join(self.movie_names, on=["tconst"])
        self.top_ten_movies.show()

    def get_top_ten_movies_ids_to_list(self):
        """makes a list of from dataframe from get_criteria_for_ranking_movies()"""
        self.movie_ids = self.top_ten_movies.select("tconst").collect()
        self.needed_movie_ids = [n[0] for n in self.movie_ids]

    def get_alternative_names_for_needed_movies(self):
        """getting alternative movie names from file.
        creating new dataframe with merged top 10 movies from get_criteria_for_ranking_movies(). var merged_via_join
        showing movies with alternative names in list"""
        self.get_top_ten_movies_ids_to_list()
        df_akas = spark.read.csv("title.akas.tsv.gz", header=True, sep=r"\t").select(
            "titleId", "title"
        )
        merged_via_join = df_akas.join(
            self.top_ten_movies,
            df_akas["titleId"] == self.top_ten_movies["tconst"],
            "inner",
        )

        movies_with_alternatives = merged_via_join.groupBy("titleId").agg(
            F.collect_list("title").alias("alternativeNames")
        )
        movies_with_alternatives.join(
            self.movie_names,
            self.movie_names["tconst"] == movies_with_alternatives["titleId"],
        ).show()

    def get_all_actors_for_needed_movies(self):
        """
        reading all actors from file.
        filtering only actors which appeared in the top 10 movies.
        reading all awards for actors in the top 10 movies
        merging actors plus their awards for the top 10 movies"""
        actors = spark.read.csv("name.basics.tsv.gz", header=True, sep=r"\t")
        needed_actors = actors.where(
            actors["knownForTitles"].rlike(
                "|".join(["(" + pat + ")" for pat in self.needed_movie_ids])
            )
        )
        awards = spark.read.csv("title.principals.tsv.gz", header=True, sep=r"\t")
        awards_for_needed_movies = awards.filter(
            (awards["tconst"].isin(self.needed_movie_ids))
        )
        self.actors_plus_awards = needed_actors.join(
            awards_for_needed_movies, on=["nconst"]
        )

    def get_most_credited_actors(self):
        """
        merging actors with their awards wthich the name of the movie based on tconst
        showing most awarded actors"""
        self.get_all_actors_for_needed_movies()
        final_df = self.actors_plus_awards.join(self.movie_names, on="tconst").select(
            "tconst", "primaryName"
        )
        final_df.groupBy("primaryName").count().sort("count", ascending=False).show()

    def run_manipulations_spark(self):
        self.get_criteria_for_ranking_movies()
        self.get_alternative_names_for_needed_movies()
        self.get_most_credited_actors()
