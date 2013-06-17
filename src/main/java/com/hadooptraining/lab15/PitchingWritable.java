package com.hadooptraining.lab15;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custom writable object to store pitching record for a player.
 */
public class PitchingWritable implements Writable {

    private Text playerID, year;
    private IntWritable runsAllowed;

    public PitchingWritable() {
        // TODO STUDENT
    }

    public void set (String playerID, String year, int runsAllowed) {
        // TODO STUDENT
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO STUDENT
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO STUDENT
    }

    public int hashCode() {
        // TODO STUDENT
        return 0; // TODO STUDENT REMOVE THIS LINE
    }

    public Text getPlayerID() {
        return playerID;
    }

    public Text getYear() {
        // TODO STUDENT
        return new Text(); // TODO STUDENT REMOVE THIS LINE
    }

    public IntWritable getRunsAllowed() {
        // TODO STUDENT
        return new IntWritable(); // TODO STUDENT REMOVE THIS LINE
    }

}

/*
      PITCHING TABLE
      ==============
1     playerID       Player ID code
2     yearID         Year
3     stint          player's stint (order of appearances within a season)
4     teamID         Team
5     lgID           League
6     W              Wins
7     L              Losses
8     G              Games
9     GS             Games Started
10     CG             Complete Games
11    SHO            Shutouts
12    SV             Saves
13    IPOuts         Outs Pitched (innings pitched x 3)
14    H              Hits
15    ER             Earned Runs
16    HR             Homeruns
17    BB             Walks
18    SO             Strikeouts
19    BAOpp          Opponent's Batting Average
20    ERA            Earned Run Average
21    IBB            Intentional Walks
22    WP             Wild Pitches
23    HBP            Batters Hit By Pitch
24    BK             Balks
25    BFP            Batters faced by Pitcher
26    GF             Games Finished
27    R              Runs Allowed
28    SH             Sacrifices by opposing batters
29    SF             Sacrifice flies by opposing batters
30    GIDP           Grounded into double plays by opposing batter

*/