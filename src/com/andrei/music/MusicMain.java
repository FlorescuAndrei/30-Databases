package com.andrei.music;

import com.andrei.music.model.Artist;
import com.andrei.music.model.SongArtist;

import java.util.List;
import java.util.Scanner;

public class MusicMain {
    public static void main(String[] args) {

        DataSource dataSource = new DataSource();

        // open dataSourceLight
        if(!dataSource.open()){
            System.out.println("Can't open database");
            return;
        }


        List<Artist> artists = dataSource.queryArtists(DataSource.ORDER_BY_NONE);
        if(artists == null){
            System.out.println("No artists");
        }
        for(Artist artist: artists){
            System.out.println(artist);
        }

//        List<String>albumsForArtist = dataSourceLight.queryAlbumsForArtists ("Queen", DataSourceLight.ORDER_BY_ASC);
//        for(String album: albumsForArtist){
//            System.out.println(album);
//        }

        System.out.println("=================================");

        List<SongArtist> songArtists = dataSource.queryArtistsForSong("She's On Fire");
        if(songArtists == null){
            System.out.println("Couldn't find the artist for the song");
            return;
        }
        for(SongArtist artist: songArtists){
            System.out.println(artist);
        }

        System.out.println("=================================");

        dataSource.querySongsMetadata();

        System.out.println("=================================");

        //function count
        int count = dataSource.getCount(DataSource.TABLE_SONGS);
        System.out.println("Number of songs is " + count);

        System.out.println("==================================");
        //Query view
        //SQL Injection Attack
        System.out.println(DataSource.CREATE_ARTIST_FOR_SONG_VIEW);
        dataSource.createViewForSongArtists();

        //SQL Injection Attack
        // Go Your Own Way" or 1=1 or "  -enter from the console
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter a song title: ");
        String title = scanner.nextLine();

        songArtists = dataSource.querySongInfoView(title);
        if(songArtists.isEmpty()){
            System.out.println("Couldn't find the artist for the song");
            return;
        }

        for(SongArtist artist : songArtists){
            System.out.println("From View - " + artist);
        }

        //Injection Attack can not be done because we use prepared statement
        // Go Your Own Way" or 1=1 or "  -enter from the console will not work
        scanner = new Scanner(System.in);
        System.out.println("Enter a song title: ");
        String title2 = scanner.nextLine();

        songArtists = dataSource.querySongInfoView2(title2);
        if(songArtists.isEmpty()){
            System.out.println("Couldn't find the artist for the song");
            return;
        }

        for(SongArtist artist : songArtists){
            System.out.println("From View - " + artist);
        }

        System.out.println("=============================");

        //Insert a song
        System.out.println("Inserting new song...");
        dataSource.insertSong("test3 song", "test3 artist", "test3 album", 7);





        //close dataSourceLight
        dataSource.close();

    }
}
