package com.andrei.music;

import com.andrei.music.model.Artist;
import com.andrei.music.model.SongArtist;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//This is a simplified short version of DataSource class
//1. Query for Artists
//2. Query Albums by Artists
//3. Query Artists for Songs
//4. Result Set Metadata
//5. Function count
//7. Create View
//8. Query View
//9. SQL Injection Attacks and Prepared Statements
//10. Transaction and Inserts -  create a transaction by inserting  artist, album, song and commit all after inserting song.

public class DataSourceLight {

    private Connection conn;

    //open database
    public boolean open(){
        String jdbcUrl =  "jdbc:mysql://localhost:3306/music?useSSL=false&serverTimezone=UTC";
        String user = "root";
        String pass = "root";
        try{
            conn = DriverManager.getConnection(jdbcUrl, user, pass);
            return true;
        } catch (SQLException e) {
            System.out.println("could not open database");
            e.printStackTrace();
            return false;
        }
    }

    //close database
    public void close(){
        try{
            if(conn != null){
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println("conn not closed");
            e.printStackTrace();
        }
    }

    //First query: select * from artists
    public List<Artist> queryArtists() {
        String query = "select * from artists";

        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(query)){

            List<Artist> artists = new ArrayList<>();
            while(resultSet.next()){
                Artist artist = new Artist();
                artist.setId(resultSet.getInt("_id"));
                artist.setName(resultSet.getString("name"));

                artists.add(artist);
            }
            return artists;

        }catch (SQLException e){
            e.printStackTrace();
            return null;
        }
    }

    //Second Query - retrieve all the albums for an artist
    // SELECT albums.name FROM albums INNER JOIN artists ON albums.artist = artists._id WHERE artists.name = "Queen"
    public List<String> queryAlbumsForArtists(String artistName){
        StringBuilder sb = new StringBuilder();
        sb.append("select albums.name from albums inner join artists on albums.artist = artists._id where artists.name = \"");
        sb.append(artistName);
        sb.append("\"");

        System.out.println("\n" + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sb.toString())) {

            List<String>albums = new ArrayList<>();
            while (resultSet.next()){
               albums.add(resultSet.getString(1));
            }
            return albums;

        }catch (SQLException e){
            e.getMessage();
            return null;
        }
    }

    //Third query retrieve the artists for a song name;
    //select artists.name, albums.name, songs.track from songs inner join albums on songs.album = albums._id
    // inner join artists on albums.artist = artists._id where songs.title = "She's On Fire";
    //String songTitleInjectionAttack = "She's On Fire\" or 1=1 or \"";
    public List<SongArtist>queryArtistsForSong(String songName){
        String query = "select artists.name, albums.name, songs.track from songs inner join albums on songs.album = albums._id " +
                "inner join artists on albums.artist = artists._id where songs.title = \"";
        StringBuilder sb = new StringBuilder(query);
        sb.append(songName);
        sb.append("\"");
        System.out.println("\n" + sb.toString());

        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString())){

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
            e.getMessage();
            return null;
        }

    }


    //Metadata
    //Retrieve columns name from the song table
    //ResultSet index start at 1;
    public void querySongMetadata(){
        String sql = "select * from songs";
        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)){

            ResultSetMetaData metaData = resultSet.getMetaData();
            int numColumns = metaData.getColumnCount();

            for(int i = 1 ; i <= numColumns; i++){
                System.out.format("Column %d in songs table is named %s\n", i, metaData.getColumnName(i));
            }

        }catch (SQLException e){
            e.printStackTrace();
        }
    }



    //count
    public int getCount(String table){
        String sql = "select count(*) as count, min(_id) as min_id from " + table;

        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)){

            resultSet.next();
            int count = resultSet.getInt(1);
            int min = resultSet.getInt("min_id");
            System.out.format("\nCount = %d, MinId = %d\n", count, min);
            return count;

        }catch (SQLException e){
            e.printStackTrace();
            return -1;
        }
    }


    //View
    //CREATE OR REPLACE VIEW artist_list AS SELECT artists.name, albums.name AS album, songs.track, songs.title
    // FROM songs INNER JOIN albums ON songs.album = albums._id INNER JOIN artists ON albums.artist = artists._id
    // ORDER BY artists.name, albums.name, songs.track
    public boolean createViewForSongArtists(){
        String sql = "create or replace view artist_list as select artists.name, albums.name as album, songs.track, songs.title " +
                "from songs inner join albums on songs.album = albums._id inner join artists on albums.artist = artists._id " +
                "order by artists.name, albums.name, songs.track";
        try(Statement statement = conn.createStatement()){
            statement.execute(sql);
            return true;
        }catch (SQLException e){
            e.printStackTrace();
            return false;
        }
    }

    //Query view
    //select name, album, track from artist_list where title = "She's On Fire";
    // Injection Attack - when enter the title inject sql: select name, album, track from artist_list where title = "Go Your Own Way" or 1=1 or""
    public List<SongArtist> querySongInfoView(String title) {
        String sql = "select name, album, track from artist_list where title = \"";
        StringBuilder sb = new StringBuilder(sql);
        sb.append(title);
        sb.append("\"");
        System.out.println("\n" + sb.toString());

        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString()))  {

            List<SongArtist> songArtists = new ArrayList<>();

            while(resultSet.next()){
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(resultSet.getString(1));
                songArtist.setAlbumName(resultSet.getString(2));
                songArtist.setTrack(resultSet.getInt(3));
                songArtists.add(songArtist);
            }
            return songArtists;
        }catch (SQLException e){
            e.printStackTrace();
            return null;
        }
    }

    //avoid Sql injection , use prepared statement with sql that have a placeholder "?"
    public List<SongArtist> querySongInfoView2(String title) {

        String sql = "select name, album, track from artist_list where title = ?";

        try(PreparedStatement preparedStatement = conn.prepareStatement(sql)){
            preparedStatement.setString(1, title);
            ResultSet resultSet = preparedStatement.executeQuery();

            List<SongArtist>songArtists = new ArrayList<>();
            while(resultSet.next()){
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(resultSet.getString(1));
                songArtist.setAlbumName(resultSet.getString(2));
                songArtist.setTrack(resultSet.getInt(3));

                songArtists.add(songArtist);
            }
            return songArtists;

        }catch (SQLException e){
            e.printStackTrace();
            return null;
        }
    }

    //Transaction
    // insert artist, insert album, insert song. Only insert song will be public and responsible for the transaction
    private int insertArtist(String name) throws SQLException{
        String queryArtistSql = "select artists._id from artists where artists.name = ?";
        PreparedStatement psQueryArtist = conn.prepareStatement(queryArtistSql);

        String insertArtistSql = "insert into artists (name) values(?)";
        PreparedStatement psInsertIntoArtist = conn.prepareStatement(insertArtistSql, Statement.RETURN_GENERATED_KEYS);
        //RETURN_GENERATED_KEYS - indicate that generated keys should be made available for retrieval

        psQueryArtist.setString(1, name);
        ResultSet resultSet = psQueryArtist.executeQuery();
        //if exist return index;
        if(resultSet.next()){
            return resultSet.getInt(1);
        }
        //else insert artist
        else{
            psInsertIntoArtist.setString(1, name);
            int affectedRows = psInsertIntoArtist.executeUpdate();
            //execute update returns the number of rows affected - we expect 1 row to be affected

            if(affectedRows !=1){
                throw  new SQLException("Couldn't insert artist!");
            }
            ResultSet generatedKeys = psInsertIntoArtist.getGeneratedKeys();

            if(generatedKeys.next()){
                return generatedKeys.getInt(1);
            }else {
                throw new SQLException("Couldn't get _id for artist");
            }

        }



    }
    private int insertAlbum(String albumName, int artistId) throws SQLException{
        String queryAlbum = "select albums._id from albums where albums.name = ?";
        PreparedStatement psQueryAlbum = conn.prepareStatement(queryAlbum);
        psQueryAlbum.setString(1, albumName);

        String insertAlbum = "insert into albums (name, artist) values(?, ?)";
        PreparedStatement psInsertAlbum = conn.prepareStatement(insertAlbum, Statement.RETURN_GENERATED_KEYS);


        ResultSet resultSet = psQueryAlbum.executeQuery();
        if(resultSet.next()) {
            return resultSet.getInt(1);

        } else {
            psInsertAlbum.setString(1, albumName);
            psInsertAlbum.setInt(2, artistId);

            int affectedRows = psInsertAlbum.executeUpdate();

            if(affectedRows != 1){
                throw new SQLException("couldn't insert album");
            }

            ResultSet generatedKey = psInsertAlbum.getGeneratedKeys();
            if(generatedKey.next()){
                return generatedKey.getInt(1);
            } else {
                throw new SQLException("couldn't get id for album");
            }
        }

    }

    public void insertSong(String title, int track, String artist, String album) {
        String insertSong = "insert into songs (track, title, album) values (?, ?, ?)";

        try {
            //perform our own transaction - set autocommit to false
            conn.setAutoCommit(false);

            // get album id and artist id
            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);

            PreparedStatement psInsertIntoSongs = conn.prepareStatement(insertSong);
            psInsertIntoSongs.setInt(1, track);
            psInsertIntoSongs.setString(2, title);
            psInsertIntoSongs.setInt(3,albumId );

            int affectedRows = psInsertIntoSongs.executeUpdate();
            if(affectedRows ==1) {
                conn.commit();
            } else {
                throw new SQLException("The song insert failed");
            }
        } catch (SQLException e) {
            System.out.println("Insert song Exception " + e.getMessage());
            e.printStackTrace();

            try {
                System.out.println("Performing rollback");
                conn.rollback();
            } catch (SQLException e2){
                System.out.println("Oh! Things are really bad!" + e2.getMessage());
            }
        }finally {
            try {
                System.out.println("Resetting default commit behavior");
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                System.out.println("Couldn't reset autocommit" + e.getMessage());

            }
        }
    }








}

