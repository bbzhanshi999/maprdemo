package neuedu;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

public class PokerRandom {

    private List<String> colors = Lists.newArrayList("黑桃","红桃","方块","桃花");
    private List<String> cards = Lists.newArrayList("A","2","3","4","5","6","7","8","9","10","J","Q","K","joker");


    @Test
    public void randomOutput() throws IOException {
        Random carNumRandom = new Random();
        Random colorRandom = new Random();
        Random cardRandom = new Random();
        FileWriter fileWriter = new FileWriter("f:/pokers.txt");
        int line = 100;
        for (int i=0;i<line;i++){
            int cardNum = carNumRandom.nextInt(101);
            for(int c = 0;c<cardNum;c++){

                int colorType = colorRandom.nextInt(4);
                int cardType  =cardRandom.nextInt(14);
                fileWriter.write(colors.get(colorType)+cards.get(cardType)+" ");
            }
            fileWriter.write("\n");
        }
        fileWriter.close();
    }
}
