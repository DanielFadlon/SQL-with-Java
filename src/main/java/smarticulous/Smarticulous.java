package smarticulous;

import smarticulous.db.Exercise;
import smarticulous.db.Submission;
import smarticulous.db.User;

import javax.swing.text.View;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Smarticulous {

    /**
     * The connection to the underlying DB.
     * <p>
     * null if the db has not yet been opened.
     */
    Connection db;

    /**
     * Open the smarticulous.Smarticulous SQLite database.
     * <p>
     * This should open the database, creating a new one if necessary, and set the {@link #db} field
     * to the new connection.
     * <p>
     * The open method should make sure the database contains the following tables, creating them if necessary:
     * <p>
     * - A ``User`` table containing the following columns (with their types):
     * <p>
     * =========  =====================
     * Column          Type
     * =========  =====================
     * UserId     Integer (Primary Key)
     * Username   Text
     * Firstname  Text
     * Lastname   Text
     * Password   Text
     * =========  =====================
     * <p>
     * - An ``smarticulous.db.Exercise`` table containing the following columns:
     * <p>
     * ============  =====================
     * Column          Type
     * ============  =====================
     * ExerciseId    Integer (Primary Key)
     * Name          Text
     * DueDate       Integer
     * ============  =====================
     * <p>
     * - A ``Question`` table containing the following columns:
     * <p>
     * ============  =====================
     * Column          Type
     * ============  =====================
     * ExerciseId     Integer
     * QuestionId     Integer
     * Name           Text
     * Desc           Text
     * Points         Integer
     * ============  =====================
     * <p>
     * In this table the combination of ``ExerciseId``,``QuestionId`` together comprise the primary key.
     * <p>
     * - A ``smarticulous.db.Submission`` table containing the following columns:
     * <p>
     * ===============  =====================
     * Column             Type
     * ===============  =====================
     * SubmissionId      Integer (Primary Key)
     * UserId           Integer
     * ExerciseId        Integer
     * SubmissionTime    Integer
     * ===============  =====================
     * <p>
     * - A ``QuestionGrade`` table containing the following columns:
     * <p>
     * ===============  =====================
     * Column             Type
     * ===============  =====================
     * SubmissionId      Integer
     * QuestionId        Integer
     * Grade            Real
     * ===============  =====================
     * <p>
     * In this table the combination of ``SubmissionId``,``QuestionId`` together comprise the primary key.
     *
     * @param dburl The JDBC url of the database to open (will be of the form "jdbc:sqlite:...")
     * @return the new connection
     */
    public Connection openDB(String dburl) throws SQLException {
        db = DriverManager.getConnection(dburl);
        Statement stmt = db.createStatement();
        String [] tables = {
                "User"+
                        "(UserId INTEGER PRIMARY KEY,"+
                        " Username TEXT UNIQUE,"+
                        " Firstname TEXT,"+
                        " Lastname TEXT,"+
                        " Password TEXT)",
                "Exercise"+
                        "(ExerciseId INTEGER PRIMARY KEY,"+
                        " Name TEXT,"+
                        " DueDate INTEGER)",
                "Question"+
                        "(ExerciseId INTEGER,"+
                        " QuestionId INTEGER,"+
                        " Name TEXT,"+
                        " Desc TEXT,"+
                        " Points INTEGER,"+
                        " PRIMARY KEY(ExerciseId, QuestionId),"+
                        " FOREIGN KEY(ExerciseId) REFERENCES Exercise(ExerciseId))",
                "Submission" +
                        "(SubmissionId INTEGER PRIMARY KEY,"+
                        " UserId INTEGER,"+
                        " ExerciseId INTEGER,"+
                        " SubmissionTime INTEGER,"+
                        " FOREIGN KEY(ExerciseId) REFERENCES Exercise(ExerciseId))",
                "QuestionGrade" +
                        "(SubmissionId INTEGER,"+
                        " QuestionId INTEGER,"+
                        " Grade REAL,"+
                        " PRIMARY KEY(SubmissionId, QuestionId),"+
                        " FOREIGN KEY(SubmissionId) REFERENCES Submission(SubmissionId),"+
                        " FOREIGN KEY(QuestionId) REFERENCES Question(QuestionId))"};

        for(String table : tables){
            String createTable = "CREATE TABLE IF NOT EXISTS " + table + ";";
            stmt.execute(createTable);
        }

        return db;
    }


    /**
     * Close the DB if it is open.
     */
    public void closeDB() throws SQLException {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    // =========== User Management =============

    /**
     * Add a user to the database / modify an existing user.
     * <p>
     * Add the user to the database if they don't exist. If a user with user.username does exist,
     * update their password and firstname/lastname in the database.
     *
     * @param user
     * @return the userid.
     */
    public int addOrUpdateUser(User user, String password) throws SQLException {
        String[] inputs ={user.username, user.firstname, user.lastname, password};
        int userId = getUserId(user.username);
        PreparedStatement pstmt;
        if(userId != -1){
            String updateUser = "UPDATE User SET Firstname=?, Lastname=?, Password=? WHERE Username=?;";
            pstmt = db.prepareStatement(updateUser);
            for(int i = 1; i < 4; i++){
                pstmt.setString(i, inputs[i]);
            }
            pstmt.setString(4, user.username);
        }else{
            String addUser = "INSERT INTO User (Username, Firstname, Lastname, Password) VALUES (?,?,?,?);";
            pstmt = db.prepareStatement(addUser);
            for(int i = 0; i < 4; i++){
                pstmt.setString(i + 1, inputs[i]);
            }
        }
        pstmt.executeUpdate();
        return getUserId(user.username);
    }

    /**
     * return the user-id of the given user-name.
     *
     * @param username
     * @return int - userId
     * @throws SQLException
     */

    private int getUserId(String username) throws SQLException{
        PreparedStatement pstmt = db.prepareStatement("SELECT UserId FROM User WHERE Username=?;");
        pstmt.setString(1, username);
        ResultSet res = pstmt.executeQuery();
        if(res.next())  return res.getInt("UserId");

        return -1;
    }

    /**
     * Verify a user's login credentials.
     *
     * @param username
     * @param password
     * @return true if the user exists in the database and the password matches; false otherwise.
     * <p>
     * Note: this is totally insecure. For real-life password checking, it's important to store only
     * a password hash
     * @see <a href="https://crackstation.net/hashing-security.htm">How to Hash Passwords Properly</a>
     */
    public boolean verifyLogin(String username, String password) throws SQLException {
        String login = "SELECT * FROM User WHERE Username=? AND Password=?;";
        PreparedStatement pstmt = db.prepareStatement(login);
        pstmt.setString(1, username);
        pstmt.setString(2, password);
        ResultSet res = pstmt.executeQuery();

        if(res.next()) return true;

        return false;
    }

    // =========== Exercise Management =============

    /**
     * Add an exercise to the database.
     *
     * @param exercise
     * @return the new exercise id, or -1 if an exercise with this id already existed in the database.
     */
    public int addExercise(Exercise exercise) throws SQLException {
        String searchEx = "SELECT * FROM Exercise WHERE ExerciseId=?;";
        PreparedStatement pstmt = db.prepareStatement(searchEx);
        pstmt.setInt(1, exercise.id);
        ResultSet res = pstmt.executeQuery();
        if(res.next()) return -1;

        int dueDate = (int) exercise.dueDate.getTime();
        String addEx = "INSERT INTO Exercise (ExerciseId, Name, DueDate) VALUES (?,?,?);";
        pstmt = db.prepareStatement(addEx);
        pstmt.setInt(1, exercise.id);
        pstmt.setString(2, exercise.name);
        pstmt.setInt(3, dueDate);
        pstmt.executeUpdate();

        String addQuest = "INSERT INTO Question (ExerciseId, Name, Desc, Points) VALUES (?,?,?,?);";
        pstmt = db.prepareStatement(addQuest);
        for(Exercise.Question quest : exercise.questions){
            pstmt.setInt(1, exercise.id);
            pstmt.setString(2, quest.name);
            pstmt.setString(3, quest.desc);
            pstmt.setInt(4, quest.points);
            pstmt.executeUpdate();
        }
        return exercise.id;
    }

    /**
     * Return a list of all the exercises in the database.
     * <p>
     * The list should be sorted by exercise id.
     *
     * @return
     */
    public List<Exercise> loadExercises() throws SQLException {
        List<Exercise> exercises = new ArrayList<>();
        Statement stmtEx = db.createStatement();
        PreparedStatement pstmtQuest = db.prepareStatement("SELECT * FROM Question WHERE ExerciseId=?;");
        ResultSet resEx = stmtEx.executeQuery("SELECT * FROM Exercise");

        int exId, points;
        String exName, questName, desc;
        long dueDateMilliseconds;
        Date dueDate;

        while(resEx.next()){
            exId = resEx.getInt("ExerciseId");
            exName = resEx.getString("Name");
            dueDateMilliseconds = resEx.getInt("DueDate");
            dueDate = new Date(dueDateMilliseconds);
            Exercise tempEx = new Exercise(exId, exName, dueDate);

            pstmtQuest.setInt(1, exId);
            ResultSet resQuest = pstmtQuest.executeQuery();
            while(resQuest.next()){
                questName = resQuest.getString("Name");
                desc = resQuest.getString("Desc");
                points = resQuest.getInt("Points");
                tempEx.addQuestion(questName, desc, points);
            }
            exercises.add(tempEx);
        }

        return exercises;
    }

    // ========== Submission Storage ===============

    /**
     * Store a submission in the database.
     * The id field of the submission will be ignored if it is -1.
     * <p>
     * Return -1 if the corresponding user doesn't exist in the database.
     *
     * @param submission
     * @return the submission id.
     */
    public int storeSubmission(Submission submission) throws SQLException {
        int userId = getUserId(submission.user.username);
        if(userId == -1) return -1;

        String subId = ", SubmissionId";
        String valueId = ",?";
        if(submission.id == -1) {
            subId = "";
            valueId = "";
        }

        String storeSub = "INSERT INTO Submission ( UserId, ExerciseId, SubmissionTime"+ subId +") VALUES (?,?,?"+ valueId +");";
        long subTime = submission.submissionTime.getTime();
        PreparedStatement pstmt = db.prepareStatement(storeSub);
        pstmt.setInt(1, userId);
        pstmt.setInt(2, submission.exercise.id);
        pstmt.setLong(3, subTime);

        if(submission.id != -1) pstmt.setInt(4, submission.id);

        pstmt.executeUpdate();

        return getSubmissionIdAfterInsert(submission.id);
    }

    /**
     * return the submission-Id that currently in the database
     * after insert the submission with the given Id to database.
     *
     * NOTE: the call to the function must be immediately after insert.
     *
     * @param submissionId
     * @return int - submissionId in the database.
     * @throws SQLException
     */

    private int getSubmissionIdAfterInsert(int submissionId) throws SQLException{
        if(submissionId != -1) return submissionId;

        Statement stmt = db.createStatement();
        ResultSet res = stmt.executeQuery("SELECT MAX(SubmissionId) AS SubmissionId FROM Submission;");
        return  res.getInt("SubmissionId");
    }

    // ============= Submission Query ===============


    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the latest submission for the given exercise by the given user.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getLastSubmission(User, Exercise)}
     *
     * @return
     */
    PreparedStatement getLastSubmissionGradesStatement() throws SQLException {
       // select the userId with the given Username
        String userId = "SELECT UserId From User WHERE Username=?";

        // select the submissionId with the given username and exerciseId which has the latest time of submission
        String lastSubview =  " SELECT " +
                "SubmissionId" +
                " FROM Submission " +
                "WHERE UserId IN ( "+ userId + ") " + "AND ExerciseId=? " +
                "ORDER BY SubmissionTime DESC " +
                "LIMIT 1";

        String columns = " Submission.SubmissionId AS SubmissionId, QuestionId, Grade, SubmissionTime";
        String conditionSubId = " QuestionGrade.SubmissionId=Submission.SubmissionId";

        // select the request columns of the rows with the latest submission-time of the given user and exercise,
        // order by questionId,
        // the number of rows are the number of questions in the given exercise.
        String result = "SELECT"+
                columns +
                " FROM QuestionGrade LEFT JOIN Submission ON"+ conditionSubId +
                " WHERE Submission.SubmissionId IN ( "+ lastSubview +" ) "+
                " ORDER BY QuestionId " +
                "LIMIT (?)";
        PreparedStatement pstmt = db.prepareStatement(result);

        return pstmt;
    }

    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the <i>best</i> submission for the given exercise by the given user.
     * The best submission is the one whose point total is maximal.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getBestSubmission(User, Exercise)}
     *
     */
    PreparedStatement getBestSubmissionGradesStatement() throws SQLException {
        // select the UserId with the givenName
        String userId = " SELECT UserId FROM User WHERE Username=? ";

        String conSubmissionId = " QuestionGrade.SubmissionId = Submission.SubmissionId ";
        String conQuestionId = " QuestionGrade.QuestionId = Question.QuestionId ";
        String consExerciseId = " Submission.ExerciseId = Question.ExerciseId ";

        // select the SubmissionId with the maximum grade (in points) of the given user-name in the given exercise.
        // net-points = grade*points.
        // 1. sum all the questions net-points in each submission of the given user in the given exercise
        // 2. order by it in descending order
        // 3. take the first ( this with the biggest total grade).
        String bestSub = " SELECT "+
                " Submission.SubmissionId AS SubmissionId " +
                " FROM Submission LEFT JOIN QuestionGrade ON "+ conSubmissionId +
                " LEFT JOIN Question ON "+ conQuestionId +" AND "+ consExerciseId +
                " WHERE UserId IN ( "+ userId + ") AND Submission.ExerciseId=? "+
                " GROUP BY Submission.SubmissionId" +
                " ORDER BY SUM(Grade * Points) DESC "+
                " LIMIT 1 ";


        String reqColumns = " Submission.SubmissionId AS SubmissionId, QuestionId, Grade, SubmissionTime ";

        // select the request columns of the rows with the best submission-grade of the given user and exercise,
        // order by questionId,
        // the number of rows are the number of questions in the given exercise.
        String result = "SELECT "+
                reqColumns +
                " FROM QuestionGrade LEFT JOIN Submission ON "+ conSubmissionId +
                " WHERE Submission.SubmissionId IN ( "+ bestSub +" ) " +
                "ORDER BY QuestionId " +
                "LIMIT (?) ";

        PreparedStatement pstmt = db.prepareStatement(result);

        return pstmt;
    }


    /**
     * Return a submission for the given exercise by the given user that satisfies
     * some condition (as defined by an SQL prepared statement).
     * <p>
     * The prepared statement should accept the user name as parameter 1, the exercise id as parameter 2 and a limit on the
     * number of rows returned as parameter 3, and return a row for each question corresponding to the submission, sorted by questionId.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @return
     */
    Submission getSubmission(User user, Exercise exercise, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, user.username);
        stmt.setInt(2, exercise.id);
        stmt.setInt(3, exercise.questions.size());

        ResultSet res = stmt.executeQuery();

        boolean hasNext = res.next();
        if (!hasNext)
            return null;

        int sid = res.getInt("SubmissionId");
        Date submissionTime = new Date(res.getLong("SubmissionTime"));

        float[] grades = new float[exercise.questions.size()];

        for (int i = 0; hasNext; ++i, hasNext = res.next()) {
            grades[i] = res.getFloat("Grade");
        }

        return new Submission(sid, user, exercise, submissionTime, (float[]) grades);
    }

    /**
     * Return the latest submission for the given exercise by the given user.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @return
     */
    public Submission getLastSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getLastSubmissionGradesStatement());
    }


    /**
     * Return the submission with the highest total grade
     *
     * @param user
     * @param exercise
     * @return
     */
    public Submission getBestSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getBestSubmissionGradesStatement());
    }
}
