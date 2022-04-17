package com.andrei.music.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


//Description:
//We use music.sql database with tree tables: songs, albums, artists
//1. Query for Artists
//2. Query Albums by Artists
//3. Query Artists for Songs
//4. Result Set Metadata
//5. Function count
//7. Create View
//8. Query View
//9. SQL Injection Attacks and Prepared Statements
//10. Transaction and Inserts -  create a transaction by inserting  artist, album, song and commit all after inserting song.

//For start, 1. Query for Artists, create method open() and close() only with conn = Driver...., and method queryArtists() = first query

public class DataSource {
    public static final String DB_NAME = "music";
    public static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/" + DB_NAME + "?useSSL=false&serverTimezone=UTC";

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;


    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;

    public static final int ORDER_BY_NONE = 1;
    public static final int ORDER_BY_ASC = 2;
    public static final int ORDER_BY_DESC = 3;

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";
    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE OR REPLACE VIEW " +
            TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK + ", " + TABLE_SONGS + "." + COLUMN_SONG_TITLE +
            " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS +
            "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
            " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
            " ORDER BY " +
            TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK;



    //use for PrepareStatement to avoid sql injection attack
    // SELECT name, album, track FROM artist_list WHERE title = ?
    //" = ?" - placeholder character
    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT " + COLUMN_ARTIST_NAME + ", " +
            COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + COLUMN_SONG_TITLE + " = ?";


    public static final String INSERT_ARTIST = "insert into " + TABLE_ARTISTS + "(" + COLUMN_ARTIST_NAME + ") values(?)";
    public static final String INSERT_ALBUM = "insert into " + TABLE_ALBUMS + "(" + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ") values(?, ?)";
    public static final String INSERT_SONG = "insert into " + TABLE_SONGS + "(" + COLUMN_SONG_TRACK +", " + COLUMN_SONG_TITLE + ", " +
            COLUMN_SONG_ALBUM + ") values(?, ?, ?)";

    //query that are needed for insertions to obtain the id
    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " +
            TABLE_ARTISTS + " WHERE " + COLUMN_ARTIST_NAME + " = ?";
    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " +
            TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_NAME + " = ?";



    private Connection conn;

    private PreparedStatement querySongInfoView;

    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;

    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;


    //open database
    public boolean open() {
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING, "root", "root");
            System.out.println("Connection successful....");

            //prepareStatement for avoiding sql injection example
            querySongInfoView = conn.prepareStatement(QUERY_VIEW_SONG_INFO_PREP);

            //prepareStatement for inserts and transaction example
            //Statement.RETURN_GENERATED_KEYS will return the id for the artist which we will need when insert album.
            insertIntoArtists = conn.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = conn.prepareStatement(INSERT_ALBUM, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = conn.prepareStatement(INSERT_SONG); //we don't need the id for song

            queryArtist = conn.prepareStatement(QUERY_ARTIST);
            queryAlbum = conn.prepareStatement(QUERY_ALBUM);


            return true;
        } catch (SQLException e) {
            System.out.println("Couldn't connect to database" + e.getMessage());
            return false;
        }
    }

    //close database
    public void close() {
        try {

            //close prepareStatement
            if(querySongInfoView != null){
                querySongInfoView.close();
            }
            if(insertIntoArtists != null){
                insertIntoArtists.close();
            }
            if(insertIntoAlbums != null){
                insertIntoArtists.close();
            }
            if(insertIntoSongs != null){
                insertIntoSongs.close();
            }

            if(queryArtist != null){
                queryArtist.close();
            }
            if(queryAlbum != null){
                queryAlbum.close();
            }



            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println("Couldn't close connection: " + e.getMessage());
        }
    }

    //First query
    public List<Artist> queryArtists(int sortOrder) {
        StringBuilder sb = new StringBuilder("select * from ");
        sb.append(TABLE_ARTISTS);
        if (sortOrder != ORDER_BY_NONE) {
            sb.append(" order by ");
            sb.append(COLUMN_ARTIST_NAME);
            if (sortOrder == ORDER_BY_DESC) {
                sb.append(" desc");
            } else {
                sb.append(" asc");
            }
        }

        Statement statement = null;
        ResultSet resultSet = null;

        //better try with resources - more concise, no need for finally block
        try {
            statement = conn.createStatement();
//            resultSet = statement.executeQuery("select * from " + TABLE_ARTISTS);
            resultSet = statement.executeQuery(sb.toString());

            List<Artist> artists = new ArrayList<>();

            while (resultSet.next()) {
                Artist artist = new Artist();

//                artist.setId(resultSet.getInt(COLUMN_ARTIST_ID));
//                artist.setName(resultSet.getString(COLUMN_ARTIST_NAME));

                artist.setId(resultSet.getInt(INDEX_ARTIST_ID));
                artist.setName(resultSet.getString(INDEX_ARTIST_NAME));

                artists.add(artist);
            }

            return artists;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;

        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                System.out.println("Error closing resultSet");
            }
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                System.out.println("Error closing statement");
            }
        }
    }

    //Second Query - retrieve all the albums for an artist
    // SELECT albums.name FROM albums INNER JOIN artists ON albums.artist = artists._id WHERE artists.name = "Queen" ORDER BY albums.name ASC
    public List<String> queryAlbumsForArtists(String artistName, int sortOrder) {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(TABLE_ALBUMS);
        sb.append(".");
        sb.append(COLUMN_ALBUM_NAME);
        sb.append(" FROM ");
        sb.append(TABLE_ALBUMS);
        sb.append(" INNER JOIN ");
        sb.append(TABLE_ARTISTS);
        sb.append(" ON ");
        sb.append(TABLE_ALBUMS);
        sb.append(".");
        sb.append(COLUMN_ALBUM_ARTIST);
        sb.append(" = ");
        sb.append(TABLE_ARTISTS);
        sb.append(".");
        sb.append(COLUMN_ARTIST_ID);
        sb.append(" WHERE ");
        sb.append(TABLE_ARTISTS);
        sb.append(".");
        sb.append(COLUMN_ARTIST_NAME);
        sb.append(" = \"");
        sb.append(artistName);
        sb.append("\"");

        if (sortOrder != ORDER_BY_NONE) {
            sb.append(" ORDER BY ");
            sb.append(TABLE_ALBUMS);
            sb.append(".");
            sb.append(COLUMN_ALBUM_NAME);
            if (sortOrder == ORDER_BY_DESC) {
                sb.append(" DESC");
            } else {
                sb.append(" ASC");
            }
        }

        System.out.println("\nSQL statement = " + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sb.toString())) {

            List<String> albumns = new ArrayList<>();
            while (resultSet.next()) {
                //column index from the result set, not column index from the table
                albumns.add(resultSet.getString(1));
            }

            return albumns;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }

    }

    //Query artist for song
    //select artists.name, albums.name, songs.track from songs inner join albums on songs.album = albums._id inner join artists on albums.artist = artists._id where songs.title = "She's On Fire";
    public List<SongArtist> queryArtistsForSong(String songName) {
        String queryArtistForSongStart = "select artists.name, albums.name, songs.track from songs " +
                "inner join albums on songs.album = albums._id inner join artists on albums.artist = artists._id where songs.title = \"";


        StringBuilder sb = new StringBuilder(queryArtistForSongStart);
        sb.append(songName);
        sb.append("\"");

        System.out.println("\nSQL Statement: " + sb.toString());

        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString()) ){

            List<SongArtist> songArtists = new ArrayList<>();
            while (resultSet.next()){
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(resultSet.getString(1));
                songArtist.setAlbumName(resultSet.getString(2));
                songArtist.setTrack(resultSet.getInt(3));

                songArtists.add(songArtist);
            }

            return songArtists;

        }catch (SQLException e){
            System.out.println("Query faild: " + e.getMessage());
            return null;
        }
    }


    //Metadata
    public void querySongsMetadata(){
        String sql = "SELECT * FROM " + TABLE_SONGS;
        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)){

            ResultSetMetaData meta = resultSet.getMetaData();
            int numColumns = meta.getColumnCount();

            //first column is at position 1
            for(int i = 1; i <= numColumns; i++){
                System.out.format("Column %d in songs table is names %s\n", i, meta.getColumnName(i));
            }
        }catch(SQLException e){
            System.out.println("Query failed: " + e.getMessage());
        }
    }

    public int getCount(String table){
        String sql = "select count(*) as count, min(_id) as min_id from " + table;

        try(Statement statement = conn.createStatement(); ResultSet resultSet = statement.executeQuery(sql)){
            resultSet.next();
            int count = resultSet.getInt(1);
            int min = resultSet.getInt("min_id");

            System.out.format("Count = %d, Min = %d\n", count, min);
            return count;

        }catch (SQLException e){
            System.out.println("query failed: " +  e.getMessage());
            e.printStackTrace();
            return -1;
        }
    }

    //View
    //CREATE OR REPLACE VIEW artist_list AS SELECT artists.name, albums.name AS album, songs.track, songs.title
    // FROM songs INNER JOIN albums ON songs.album = albums._id INNER JOIN artists ON albums.artist = artists._id
    // ORDER BY artists.name, albums.name, songs.track
    public boolean createViewForSongArtists() {

        try(Statement statement = conn.createStatement()) {

            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);

            return true;

        } catch(SQLException e) {
            System.out.println("Create View failed: " + e.getMessage());
            return false;
        }
    }

    //Query view
    //select name, album, track from artist_list where title = "She's On Fire";

    // Injection Attack - when enter the title inject sql: select name, album, track from artist_list where title = "Go Your Own Way" or 1=1 or""
    public List<SongArtist> querySongInfoView(String title){
        String sql = "select name, album, track from artist_list where title = \"";
        StringBuilder sb = new StringBuilder(sql);
        sb.append(title);
        sb.append("\"");

        System.out.println(sb.toString());

        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sb.toString())){

            List<SongArtist> songArtists = new ArrayList<>();
            while (rs.next()){
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(rs.getString(1));
                songArtist.setAlbumName(rs.getString(2));
                songArtist.setTrack(rs.getInt(3));

                songArtists.add(songArtist);
            }
            return songArtists;

        }catch (SQLException e){
            System.out.println("query failed: " + e.getMessage());
            return null;
        }

    }

    //avoid Sql injection , use prepared statement wit sql that have a placeholder "?"
    public List<SongArtist> querySongInfoView2(String title){

        try{
            // 1 refer to the first position when question mark ? appear
            querySongInfoView.setString(1, title);

            ResultSet rs = querySongInfoView.executeQuery();

            List<SongArtist> songArtists = new ArrayList<>();
            while (rs.next()){
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(rs.getString(1));
                songArtist.setAlbumName(rs.getString(2));
                songArtist.setTrack(rs.getInt(3));

                songArtists.add(songArtist);
            }
            return songArtists;

        }catch (SQLException e){
            System.out.println("query failed: " + e.getMessage());
            return null;
        }

    }

    //Inserts and Transaction
    // insert artist, insert album, insert song. Only insert song will be public and responsible for the transaction
    private int insertArtist(String name) throws SQLException{
        queryArtist.setString(1, name);
        ResultSet rs = queryArtist.executeQuery();
        //if exist return index
        if(rs.next()){
            return rs.getInt(1);
        }
        //else insert artist
        else{
            insertIntoArtists.setString(1, name);
            int affectedRows = insertIntoArtists.executeUpdate();
            //execute update returns the number of rows affected - we expect 1 row to be affected

            if(affectedRows !=1){
                throw  new SQLException("Couldn't insert artist!");
            }
            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();

            if(generatedKeys.next()){
                return generatedKeys.getInt(1);
            }else {
                throw new SQLException("Couldn't get _id for artist");
            }
        }
    }

    private int insertAlbum(String name, int artistId) throws SQLException{
        queryAlbum.setString(1, name);
        ResultSet rs = queryAlbum.executeQuery();
        //if exist return index
        if(rs.next()){
            return rs.getInt(1);
        } else{
            //insert album
            insertIntoAlbums.setString(1, name);
            insertIntoAlbums.setInt(2, artistId);
            int affectedRows = insertIntoAlbums.executeUpdate();
            //execute update returns the number of rows affected - we expect 1 row to be affected

            if(affectedRows !=1){
                throw  new SQLException("Couldn't insert album!");
            }
            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if(generatedKeys.next()){
                return generatedKeys.getInt(1);
            }else {
                throw new SQLException("Couldn't get _id for album");
            }
        }
    }

    public void insertSong(String title, String artist, String album,  int track){

        try{
            //perform our own transaction - set autocommit to false
            conn.setAutoCommit(false);

            // get album id and artist id

            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);

            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2, title);
            insertIntoSongs.setInt(3, albumId);

            int affectedRows = insertIntoSongs.executeUpdate();
            if(affectedRows == 1){
                conn.commit();
            }else {
                throw new SQLException("The song insert failed");
            }


        }catch (SQLException e){
            System.out.println("Insert song exception: " + e.getMessage());
            try {
                System.out.println("Performing rollback");
                conn.rollback();
            }catch (SQLException e2){
                System.out.println("Oh! Things are really bad!" + e2.getMessage());
            }
        }finally {
            try{
                System.out.println("Resetting default commit behavior");
                conn.setAutoCommit(true);
            }catch (SQLException e){
                System.out.println("Couldn't reset auto-commit " + e.getMessage());
            }
        }



    }





}

