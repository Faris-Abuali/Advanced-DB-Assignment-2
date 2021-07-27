package advancedDBAssignment2;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Fares Abu Ali
 */
public class AdvDBAssignment2FaresAbuali {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        
   
    }// end main

    public static class Triple<T, U, V> {

        private T left;
        private U middle;
        private V right;

        public Triple(T left, U middle, V right) {
            this.left = left;
            this.middle = middle;
            this.right = right;
        }

        public void setLeft(T left) {
            this.left = left;
        }

        public void setMiddle(U middle) {
            this.middle = middle;
        }

        public void setRight(V right) {
            this.right = right;
        }

        public T getLeft() {
            return left;
        }

        public U getMiddle() {
            return middle;
        }

        public V getRight() {
            return right;
        }

        @Override
        public String toString() {

            String tripleContent = left + ", " + middle + ", " + right;

            return tripleContent;
        }

    }// end Triple

    public static class Pair<P, Q> {

        private P left;
        private Q right;

        public Pair(P left, Q right) {
            this.left = left;
            this.right = right;
        }

        public void setLeft(P left) {
            this.left = left;
        }

        public void setRight(Q right) {
            this.right = right;
        }

        public P getLeft() {
            return left;
        }

        public Q getRight() {
            return right;
        }

        @Override
        public String toString() {

            String pairContent = left + ", " + right;

            return pairContent;
        }

    }// end Pair

    public static class UsersMapper
            extends Mapper<LongWritable, Text, Text, Triple> {

        // mapper KEYIN: is the line offset (LongWritable)
        // mapper VALUEIN: is the content of this line (Text)  
        // mapper KEYOUT: is the userID (Text) 
        // mapper VALUEOUT: is  a (Triple): [gender, age, “U”]
        // the "U" tag is to indicate that this value is from the dataset called "Users"
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // value is a line with three words: userID  gender  age
            //(imagine that the line contains these 3 words which are separated by a space or a comma)
            Triple<String, String, String> triple = new Triple("", "", "");
            /* actually a PAIR is enough to store the the 'age' and the "U" tag, but I used a Triple because later I will 
            join the output of this mapper with the output of the RatingsMapper. 
            
            The Ratings dataset contains 3 records, and since both outputs of the UsersMapper and 
            the RatingsMapper will enter the same reducer, I want the reducer to have an Iterable<Triple>
             */

            String[] str_arr = value.toString().split(",");

            String strUserID = str_arr[0].trim(); //eliminate unnecessary leading and trailing spaces
            String strGender = str_arr[1].trim(); //eliminate unnecessary leading and trailing spaces
            String strAge = str_arr[2].trim();    //eliminate unnecessary leading and trailing spaces

            //Now make the SELECTION or the FILTER (age > 25)
            if (Integer.parseInt(strAge) > 25) {

                triple.setLeft(strGender);// I don't need the 'gender' attribute, but I will store it for now.
                triple.setMiddle(strAge);
                triple.setRight("U"); // a "U" tag to indicate that this value is from the dataset called "Users"

                context.write(new Text(strUserID), triple);  // context.write(key, value)

                //KEYOUT:  is the userID (which is the join attribute)
                //VALUEOUT: is the triple: [gender, age, “U”]
            }

        }// end method map

    }// end class UsersMapper

//-------------------------------------------------------------------------------------------
    public static class RatingsMapper
            extends Mapper<LongWritable, Text, Text, Triple> { // KEYIN, VALUEIN, KEYOUT, VALUEOUT
        // mapper KEYIN: is the line offset (LongWritable)
        // mapper VALUEIN: is the content of this line (Text)  
        // mapper KEYOUT: is the userID (Text) 
        // mapper VALUEOUT: is  a (Triple): [movieID, rating, “R”]
        // the "R" tag is to indicate that this value is from the dataset called "Ratings"

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // value is a line with three words: userID  movieID  rating
            //(imagine that the line contains these 3 words which are separated by a space or a comma)
            Triple<String, String, String> triple = new Triple("", "", "");

            String[] str_arr = value.toString().split(",");

            String strUserID = str_arr[0].trim(); //eliminate unnecessary leading and trailing spaces
            String strMovieID = str_arr[1].trim();
            String strRating = str_arr[2].trim();

            //Now make the SELECTION or the FILTER (rating > 2)
            if (Integer.parseInt(strRating) > 2) {

                triple.setLeft(strMovieID);
                triple.setMiddle(strRating);
                triple.setRight("R"); // an "R" tag to indicate that this value is from the dataset called "Ratings"

                context.write(new Text(strUserID), triple);  // context.write(key, value)

                //KEYOUT:  is the userID (which is the join attribute)
                //VALUEOUT: is the triple :[movieID, rating, “R”]
            }
        }// end method map

    }// end class RatingsMapper
//-------------------------------------------------------------------------------------------

    /*
    Reduce Phase:
        receive all the tuples that share the same key (which is the userID)
        from the two mappers: UsersMapper & RatingsMapper:

        - if the list of values contains the two tags U and R, this indicates that this list of values 
          is associated with a user (userID) who has achieved the two requirements: 
                1. Their age is above 25, and
                2. Their rating is above 2 stars. 
          So we will emit this key with the needed attributes: (movieID, rating)
     */
    public static class UsersJoinRatings
            extends Reducer<Text, Triple, Text, Pair> {
        // KEYIN, VALUEIN, KEYOUT, VALUEOUT

        // reducer KEYIN: userID (Text)
        /* reducer VALUEIN: an iterable list of type (Triple)
                the triple is either: [gender, age, “U”]  or [movieID, rating, “R”]*/
        // reducer KEYOUT: movieID
        // reducer VALUEOUT: Pair:[rating, “UR”] 
        @Override
        public void reduce(Text key, Iterable<Triple> values, Context context)
                throws IOException, InterruptedException {

            // reducer KEYIN: userID (Text)
            /* reducer VALUEIN: an iterable list of type (Triple)
                the triple is either: [gender, age, “U”]  or [movieID, rating, “R”]  
             */
            boolean ageIsAbove25 = false, ratingIsAbove2 = false;

            /* receive all the tuples that share the same key (which is the userID)
               from the two mappers: UsersMapper & RatingsMapper:
             */
            for (Triple<String, String, String> currentTriple : values) {

                /* VERY IMPORTANT!:
                    Iterate over the whole list of values, if the list contains BOTH tags: U and R, then
                    the 2 requirements are satisfied (age > 25 and rating >2). So Now:
                 */
                // check the tag of the record to know from which dataset it came:
                if (currentTriple.right.equals("U")) {
                    // then this value is from "Users" dataset after filtering it. So age > 25 is guaranteed.
                    ageIsAbove25 = true;
                } else if (currentTriple.right.equals("R")) {
                    // then this value is from "Ratings" dataset after filtering it. So rating > 2 is guaranteed.
                    ratingIsAbove2 = true;
                }

                if (ageIsAbove25 && ratingIsAbove2) {

                    /*the 2 requirements are satisfied (age > 25 and rating >2).
                      So no need to iterate over the next records (our goal is achieved)*/
                    break;
                }

            }// end for each

            if (ageIsAbove25 && ratingIsAbove2) {
                // if any of the 2 above conditions are not satisfied, then don't emit any value. Just skip all the reucer function!
                // because we only want to emit data about movies whose rating is > 2 and whose users are > 25

                for (Triple<String, String, String> currentTriple : values) {

                    if (currentTriple.getRight().equals("R")) {
                        //then emit the following: (movieID, (rating + “UR”))

                        //movieID is stored in currentTriple.left; 
                        // rating is stored in currentTriple.middle;
                        String movieID = currentTriple.getLeft();
                        String rating = currentTriple.getMiddle();

                        /* we may need to print a tag “UR” for example, to indicate that this data
                       is obtained by joining the two datasets: Users “U” and Ratings “R”:*/
                        Pair<String, String> pair = new Pair("", "");
                        pair.setLeft(rating);
                        pair.setRight("UR");
                        context.write(new Text(movieID), pair);   // (movieID,  pair: [rating + “UR”)] )
                        // KEYOUT:=   movieID, VALUEOUT:=   pair: [rating + “UR”)]

                    }// end if
                    else if (currentTriple.getRight().equals("U")) {
                        // do nothing, because we don’t want to emit any data of this triple: [gender, age, “U”]  
                    }

                }// end for each
            }

        }// end method reduce

    }// end class UsersJoinRatings

    public static class MoviesMapper
            extends Mapper<LongWritable, Text, Text, Pair> {// KEYIN, VALUEIN, KEYOUT, VALUEOUT

        // mapper KEYIN: is the line offset (LongWritable)
        // mapper VALUEIN: is the content of this line (Text): movieID  title  genres
        // mapper KEYOUT: is the movieID (Text) 
        // mapper VALUEOUT: is  a (Pair): [title, “M”]  
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // value is a line with three words: movieID  title  genres
            //(imagine that the line contains these 3 words which are separated by a space or a comma)
            Pair<String, String> pair = new Pair("", "");

            String[] str_arr = value.toString().split(",");

            String strMovieID = str_arr[0].trim();
            String strTitle = str_arr[1].trim();
            String strGenres = str_arr[2].trim();

            //Now make the SELECTION or the FILTER (only children and comedy movies)
            if (strGenres.toLowerCase().equals("children") || strGenres.toLowerCase().equals("comedy")) {

                pair.setLeft(strTitle);
                pair.setRight("M");

                context.write(new Text(strMovieID), pair); //context.write(key, value) 
                // key:= movieID,  value:= pair [title, “M”]

            }// end if

        }// end method map

    }// end class MoviesMapper

    public static class MoviesReducer
            extends Reducer<Text, Pair, Text, Text> {

        // KEYIN, VALUEIN, KEYOUT, VALUEOUT
        // reducer KEYIN: is the movieID (Text)
        /* reducer VALUEIN: an iterable list of type (Pair)
                the pair is either: [rating, “UR”]  or [title, “M”]  
        
            - "UR" indicates that this record came from joining the "Users" with "Ratings".
            - "M" tag indicates that this record came from "Movies" dataset
         */
        // reducer KEYOUT:    movieID
        // reducer VALUEOUT:   Pair:  [title, avgRating] 
        @Override
        public void reduce(Text key, Iterable<Pair> values, Context context) 
                throws IOException, InterruptedException {

            // reducer KEYIN: movieID (Text)
            /* reducer VALUEIN: an iterable list of type (Pair)
                    the pair is either: [rating, “UR”]  or [title, “M”]  
             */
            // reducer KEYOUT:    movieID
            // reducer VALUEOUT:   Pair:  [title, avgRating]
            /* receive all the tuples that share the same key (which is the movieID)
             from the two mappers: MoviesMapper & UsersJoinRatingsMapper:*/
            boolean foundTagM = false, foundTagUR = false;

            for (Pair currentPair : values) {

                /* VERY IMPORTANT!:      
                    Iterate over the whole list of values, if the list contains both tags: “M” and “UR”, then this means all the 3 requirements are satisfied:
                       1. This movie’s genre is either “comedy” or “children”.
                       2. Ratings above 2 stars are considered only.
                       3. Ratings by users older than 25 years are considered only.
                 */
                // check the tag of the record to know from which dataset it came:
                if (currentPair.right.equals("M")) {
                    // then this value is from "Movies" dataset after filtering it. So genres is either "comedy" or "children".
                    foundTagM = true;
                } else if (currentPair.right.equals("UR")) {
                    // then this value is from joining "Users" with "Ratings" dataset after filtering it. 
                    // So the conditions (age >25) and (rating > 2) is guaranteed.
                    foundTagUR = true;
                }

                if (foundTagM && foundTagUR) {

                    //no need to iterate over the next records (our goal is achieved)*/
                    break;
                }

            }// end for each

            if (foundTagM && foundTagUR) {
                // if any of the 2 above conditions are not satisfied, then don't emit any value. Just skip all the reucer function!
                // because we only want to emit data about movies that satisfy all the requirements.

                float sum = 0f;
                int cnt = 0;
                String strTitle = "";

                for (Pair<String, String> currentPair : values) {

                    /* "values" is an iterable list of Pairs 
                     (each pair contains either:
                        [title, “M”] if the value is from the “Movies” dataset
		    or [rating, “UR”] if the value is from the “UsersJoinRatings” dataset.
                     */
                    //So “currentPair”  is an object.
                    //remember: the relationship between Movies -> UsersJoinRatings is 1 -> Many
                    // so I know that only one Pair with the tag “M” is in the iterable list, 
                    //because the movieID (which is the key) is the primary key in the “Movies” dataset
                    if (currentPair.right.equals("M")) {
                        strTitle = currentPair.right;
                        /* store the movie’s title temporarily in order to cross it with other attributes 
                           from the tag “UR”, which are ‘movieID’ and ‘rating’*/
                    } 
                    else if (currentPair.right.equals("UR")) {
                        // then this pair is from the “UsersJoinRatings” dataset. Pair: [rating, “UR”]

                        sum += Float.parseFloat(currentPair.left);// currentPair.left is the rating
                        cnt += 1;
                    }

                }// end for each

                /* now I know the title of the movie, and I know the number of the 
                ratings for this specific movie,and the sum of these ratings.*/
                
                
                
                //The final output: emit (movieID, title, avgRating) 

                String finalOutput = strTitle + ", " + (sum/cnt);
                
                context.write(key, new Text(finalOutput)); //remember: movieID is the key of the reducer
                
            }// end if

        }// end method reduce

    }// end class MoviesReducer

}// end class
