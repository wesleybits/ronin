package ronin.helper;

import kyotocabinet.*;

/** ByteArray helper object
 *  This is a generally stateful object that will help deal with 2D byte arrays,
 *  which can be difficult to handle in Clojre.
 */
public class ByteArray {
    private byte[][] contents;
    private int curRow;

    public ByteArray(int height, int width) {
	contents = new byte[height][width];
	curRow = 0;
    }
    
    public void push(byte[] newRow) {
	for (int i = 0; i < newRow.length; i++) {
	    contents[curRow][i] = newRow[i];
	}
	curRow ++;
    }

    public byte[][] drop() {
	return contents;
    }
}
