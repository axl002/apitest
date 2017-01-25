/**
 * Created by user on 1/25/17.
 */
public class CurrencyConverter {

    public static void main(String[] args){
        System.out.println(parseValue("~b/o 999exa"));
    }
    static double parseValue(String note){
        String dankNote = note.toLowerCase();
        // check if buyout
        if(dankNote.contains("~b/o".toLowerCase())){
            if (dankNote.contains("chaos")){
                return Double.parseDouble(dankNote.replaceAll("[^0-9]+", ""))/72;
            }
            else if (dankNote.contains("exa")){
                return Double.parseDouble(dankNote.replaceAll("[^0-9]+", ""));
            }
            else {
                return -1;
            }
        }
        return -2;
    }
}
