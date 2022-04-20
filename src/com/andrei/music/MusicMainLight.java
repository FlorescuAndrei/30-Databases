package com.andrei.music;

import java.util.List;

public class MusicMainLight {

    public static DataSourceLight dataSourceLight = new DataSourceLight();
    public static void main(String[] args) {
        dataSourceLight.open();

        printList(dataSourceLight.queryArtists());
        printList(dataSourceLight.queryAlbumsForArtists("Metallica"));

        String songTitle = "She's On Fire";
        String songTitleInjectionAttack = "She's On Fire\" or 1=1 or \"";

        //can do injection attack
        printList(dataSourceLight.queryArtistsForSong(songTitle));

        dataSourceLight.querySongMetadata();
        dataSourceLight.getCount("songs");

        System.out.println("\nCreate view: " + dataSourceLight.createViewForSongArtists());

        System.out.println("\nCan do injection attack - Statement");
        //can do injection attack - Statement
        printList(dataSourceLight.querySongInfoView(songTitleInjectionAttack));

        System.out.println("\nCanNot do injection attack - PreparedStatement");
        //cannot do injection attack - PreparedStatement
        printList(dataSourceLight.querySongInfoView2(songTitleInjectionAttack));

        //Insert a song
        System.out.println("\nInserting new song...");
        dataSourceLight.insertSong("test5 song", 7, "test artist", "test5 album");

        System.out.println("\nDone!");
        dataSourceLight.close();


    }

    private static void printList(List list){
        list.forEach(System.out::println);
    }

}
