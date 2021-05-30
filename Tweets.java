/*
	Class-1 for id
*/
public class Id {
 private String oid;


 // Getter Methods 
 public String getoid() {
  return oid;
 }

 // Setter Methods
 public void setoid(String oid) {
  this.oid = oid;
 }
}


/*
	Class-2 for Tweets
*/
public class Tweets {
 Id _idObject;
 private String user_id;
 private String social_feed;
 private String timestamp;
 private String location_type;
 ArrayList < Object > coordinates = new ArrayList < Object > ();
 private int sentiment_score;


 // Getter Methods 

 public Id get_id() {
  return _idObject;
 }

 public String getUser_id() {
  return user_id;
 }

 public String getSocial_feed() {
  return social_feed;
 }

 public String getTimestamp() {
  return timestamp;
 }

 public String getLocation_type() {
  return location_type;
 }

  public int getsentiment_score() {
  return sentiment_score;
 }


 // Setter Methods 
 public void set_id(Id _idObject) {
  this._idObject = _idObject;
 }

 public void setUser_id(String user_id) {
  this.user_id = user_id;
 }

 public void setSocial_feed(String social_feed) {
  this.social_feed = social_feed;
 }

 public void setTimestamp(String timestamp) {
  this.timestamp = timestamp;
 }

 public void setLocation_type(String location_type) {
  this.location_type = location_type;
 }

 public void setsentiment_score(int sentiment_score) {
  this.sentiment_score = sentiment_score;
 }
}

