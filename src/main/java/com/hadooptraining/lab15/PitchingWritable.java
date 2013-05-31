package com.hadooptraining.lab15;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PitchingWritable implements Writable {

    private Text playerID, year;
    private IntWritable runsAllowed;

    public PitchingWritable() {
        this.playerID = new Text();
        this.year =  new Text();
        this.runsAllowed = new IntWritable();
    }

    public void set (String playerID, String year, int runsAllowed) {
        this.playerID.set(playerID);
        this.year.set(year);
        this.runsAllowed.set(runsAllowed);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        playerID.readFields(in);
        year.readFields(in);
        runsAllowed.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        playerID.write(out);
        year.write(out);
        runsAllowed.write(out);
    }

    public int hashCode() {
        return playerID.hashCode();
    }

    public Text getPlayerID() {
        return playerID;
    }

    public Text getYear() {
        return year;
    }

    public IntWritable getRunsAllowed() {
        return runsAllowed;
    }

}

/*
      PITCHING TABLE
      ==============
0     playerID       Player ID code
1     yearID         Year
2     stint          player's stint (order of appearances within a season)
3     teamID         Team
4     lgID           League
5     W              Wins
6     L              Losses
7     G              Games
8     GS             Games Started
9     CG             Complete Games
10    SHO            Shutouts
11    SV             Saves
12    IPOuts         Outs Pitched (innings pitched x 3)
13    H              Hits
14    ER             Earned Runs
15    HR             Homeruns
16    BB             Walks
17    SO             Strikeouts
18    BAOpp          Opponent's Batting Average
19    ERA            Earned Run Average
20    IBB            Intentional Walks
21    WP             Wild Pitches
22    HBP            Batters Hit By Pitch
23    BK             Balks
24    BFP            Batters faced by Pitcher
25    GF             Games Finished
26    R              Runs Allowed
27    SH             Sacrifices by opposing batters
28    SF             Sacrifice flies by opposing batters
29    GIDP           Grounded into double plays by opposing batter

*/