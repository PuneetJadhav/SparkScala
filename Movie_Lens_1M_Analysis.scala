package org.apache.spark.dataframes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Movie_Lens {
 
  def main(args:Array[String]){
      val sc= new SparkContext("local[*]","sparkContext")
      val sparksession = SparkSession.builder().appName("sparksession").getOrCreate()
      import sparksession.implicits._
      val movie_rdd_temp=sc.textFile("C:/College/Hadoop/ml-1m/ml-1m/movies.dat")
      val movies_rdd = movie_rdd_temp.map(x=>x.split("::")).map(x=> (x(0),x(1),x(2)))
      val movie_labels=Seq("MovieId","MovieName","Genre")
      val movies_df = movies_rdd.toDF(movie_labels:_*).cache()
      val users_movies_temp=sc.textFile("C:/College/Hadoop/ml-1m/ml-1m/ratings.dat")
      val users_movies_rdd =users_movies_temp.map(x=>x.toString()).map(x=>x.split("::")).map(x=>(x(0),x(1),x(2).toInt))
      val users_movies_labels=Seq("MovieId","UserId","Ratings")
      val users_movies_df=users_movies_rdd.toDF(users_movies_labels:_*).cache()
      val users_rdd_temp =sc.textFile("C:/College/Hadoop/ml-1m/ml-1m/users.dat")
      val users_rdd =users_rdd_temp.map(x=>x.toString()).map(x=>x.split("::")).map(x=>(x(0),x(1),x(2),x(3),(4)))
      val users_labels=Seq("UserId","Gender","Age","Occupation","ZipCode")
      val users_df=users_rdd.toDF(users_labels:_*).cache()
      val join_data_temp=movies_df.join(users_movies_df, Seq("MovieId"))
      val join_data=join_data_temp.join(users_df,Seq("UserId")).cache()
      join_data.createOrReplaceTempView("Movies_table")
      // Perform various analysis on dataset now
      //Top rated movies on the basis of ratings and ratings count as well where ratings count will be min 1000
      var top_movies_temp=sparksession.sql("select MovieId,MovieName,Genre,sum(Ratings) as Ratings_sum,count(MovieId) as Ratings_Count from Movies_table group by MovieId,MovieName,Genre order by Ratings_Count desc")
      top_movies_temp.createOrReplaceTempView("Movies_table")
      top_movies_temp=sparksession.sql("select MovieId,MovieName,Genre,Ratings_Count,(Ratings_sum/Ratings_Count) as Average_Ratings from Movies_table where Ratings_Count>1000 order by Average_ratings desc")
      top_movies_temp.show()
      //The user who has rated the most movies
       join_data.createOrReplaceTempView("Movies_Users")
      var top_user_rated= sparksession.sql("select UserId,count(UserId) as No_Movies_Rated from Movies_Users group by UserId order by No_Movies_Rated desc")
      top_user_rated.show()
       //The Gender who has rated the most movies
       join_data.createOrReplaceTempView("Movies_Gender")
      var gender_rated= sparksession.sql("select Gender,count(Gender) as No_Movies_Rated from Movies_Users group by Gender order by No_Movies_Rated desc")
      gender_rated.show()
      //Occupation ratings for movies
       join_data.createOrReplaceTempView("Movies_Occupations")
      var occupation_rated= sparksession.sql("select Occupation,count(Occupation) as No_Movies_Rated from Movies_Users group by Occupation order by No_Movies_Rated desc")
      occupation_rated.show()
  }
  

}
