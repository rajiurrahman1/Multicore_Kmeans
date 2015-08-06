import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

class RunnableDemo implements Runnable {
    private Thread t;
    private String threadName;
    private volatile int retValue;
    private volatile int[] retArray = new int[5];
    double[][] centoirdsInsideThread = {{1.0}, {2.0}};
    double[][] dataInsideThread = {{1.0}, {2.0}};
    double[][] distancesInsideThread = {{1.0}, {2.0}};
    
    private int kInsideThread, numColInsideThrad;
   
    RunnableDemo( String name){
       threadName = name;
//       System.out.println("Creating " +  threadName );
   }
    public void run(){
        for(int i=0; i<5; i++){
            retArray[i]++;
        }                
        
//        for(int i=0; i<this.dataInsideThread.length; i++){
//            for(int j=0; j<this.numColInsideThrad; j++){
//                System.out.print(/*t.getName()+"-"+ i+"-"+j+"-"+*/this.dataInsideThread[i][j] + " ");
//            }
//            System.out.println("\nprinted row=-=-=-=-=-=-=-=-=-=");
//        }
        int totalDataRowsInThread = this.dataInsideThread.length;
        distancesInsideThread = new double[totalDataRowsInThread][kInsideThread];
        distancesInsideThread = getDistance(); 
      
    }
    public double[][] getDistanceFromThread() throws InterruptedException{
        t.join();
        return this.distancesInsideThread;
    }
    public int[] getArray() throws InterruptedException{
       t.join();
       return retArray;
    }   
    public void setArray(int multFactor){
        for(int i=0; i<this.retArray.length; i++){
            this.retArray[i] = i*multFactor;
        }
   }   
    public void setCentroids(int k, int numCol, double[][] centroids){
        this.centoirdsInsideThread = new double[k][numCol];
        this.kInsideThread = k;
        this.numColInsideThrad = numCol;
        for(int i=0; i<k; i++){
            for(int j=0; j<numCol; j++){
                this.centoirdsInsideThread[i][j] = centroids[i][j];
            }
        }
    }      
    public void setData(int startIndex, int endIndex, int numCol, double[][] data){
        this.dataInsideThread = new double[(endIndex-startIndex)][numCol];
        for(int i=0; i<(endIndex-startIndex); i++){
            for(int j=0; j<numCol; j++){
                this.dataInsideThread[i][j] = data[startIndex+i][j];
            }
        }
    }    
    public void start() throws InterruptedException{
      if (t == null){
         t = new Thread (this, threadName);
         t.start ();
      }
   }
    private double[][] getDistance() {
        int totalDataRowsInThread = this.dataInsideThread.length;
        double[][] distances = new double[totalDataRowsInThread][kInsideThread];
        System.out.println("In Thread: "+ t.getName()+" \t Processing "+ totalDataRowsInThread + " Rows" );
        for(int i=0; i<totalDataRowsInThread; i++){
            for(int j=0; j<kInsideThread; j++){
                distances[i][j] = calculateDistanceOfRows(i, j); //i'th row of data, j'th row of centroids
            }
        }
        
        return distances;
    }
    private double calculateDistanceOfRows(int dataRow, int centroidRow) {
        double d1 = 0;
        for(int i=0; i<this.numColInsideThrad; i++){
            d1 += Math.pow(  (this.dataInsideThread[dataRow][i] - this.centoirdsInsideThread[centroidRow][i]), 2);
        }
        return Math.sqrt(d1);
    }
   
}

public class TestThread {    
    
    public static boolean isNumeric(String str){  
        try{
            double d = Double.parseDouble(str);  
        }  
        catch(NumberFormatException nfe){  
            return false;  
        }  
        return true;  
    }    
    public static double[][] readData(int numRow, int numCol, String dataFileName ) throws FileNotFoundException{
        
        //helping link http://stackoverflow.com/questions/22185683/read-txt-file-into-2d-array
        double [][] matrix = new double[numRow][numCol];
        String filename = dataFileName;
        File inFile = new File(filename);
        Scanner in = new Scanner(inFile);
     
        int lineCount = 0;
        while (in.hasNextLine()) {
            String[] currentLine = in.nextLine().trim().split("\\s+"); 
            for (int i = 0; i < currentLine.length; i++) {
                if(isNumeric(currentLine[i])){
                      matrix[lineCount][i] = Double.parseDouble(currentLine[i]);    
                }
                else{
                      matrix[lineCount][i] = (double)0.0;    
                }
            }
            lineCount++;
        }
        
        return matrix;
    }    
    public static double[][] getCentoids(double[][] data, int k, int numCol){
        double[][] centroids = new double[k][numCol];
        for(int i=0; i<k; i++){
            for(int j=0; j<numCol; j++){
                centroids[i][j] = data[i][j];
            }
        }
        return centroids;
    }
    private static int[] getClosestCentroid(double[][] r1Distance, int numRowsInData, int k) {        
        // this variable cc - ClosestCentroids will store the index of the closest centroids for each data row
        int[] cc = new int[numRowsInData]; 
        for(int i=0; i<numRowsInData; i++){  
            cc[i] = getClosestCentroidIndexForDataRow(r1Distance, i, k); 
        }
        
        return cc;
    }    
    private static int getClosestCentroidIndexForDataRow(double[][] r1Distance, int dataRowNum, int k) {
        //find the minimum distance and return the index of that distance
        int minIndex = 0;
        double minDistance = Double.POSITIVE_INFINITY;
        for(int i=0; i<k; i++){
            if( r1Distance[dataRowNum][i] < minDistance){
                minIndex = i;
                minDistance = r1Distance[dataRowNum][i];
            }
        }
        
        return minIndex;
    }
    private static void dumpMatrix(double[][] centroids, int k, int numCol) {
        System.out.println("Printing Matrix");
        for(int i=0; i<k; i++){
            for(int j=0; j<numCol; j++){
                System.out.print(centroids[i][j]+" ");
            }
            System.out.println();
        }
    }    
    private static void dumpArray(int[] arr) {
        System.out.println("Dumping Array");
        for(int i=0; i<arr.length; i++){
            System.out.println(arr[i]);
        }
    }
    private static double[][] addDataPointsToNewCentroid(double[][] matrix, double[][]newCentroids , int[] closestCentroids, int minIndex, int maxIndex, int numCol){
        // add the data points to the cluster they belong to
        for(int i=0; i<(maxIndex - minIndex); i++){
            //add (minIndex+i)th data row to it's closest centroid
            int closesetCentroidIndex = closestCentroids[i]; 
            for(int j=0; j<numCol; j++){
                newCentroids[closesetCentroidIndex][j] = newCentroids[closesetCentroidIndex][j]+matrix[(minIndex+i)][j];
            }
        }

        return newCentroids;
    }
    private static int[] addToNewCentroidMemberCount(int[] newCentroidMemberCount, int[] closestCentroids) {
        // Add count about how many data points have associated to which centroid
        // purpose is to divide at the end of adding all the data points to cluster centroids, to get new centroid
        for(int i=0; i<closestCentroids.length; i++){
            int centroidIndex = closestCentroids[i];
            newCentroidMemberCount[centroidIndex]++;
        }
        
        return newCentroidMemberCount;
    }
    private static double[][] divideCentroidByCount(double[][] newCentroids, int[] newCentroidMemberCount, int k, int numCol) {
        // first take the count how many instances belong to a cluster
        // then divide all the values in the centroid by the count, kind of taking average
        for(int i=0; i<k; i++){
            int centroidMembershipCount = newCentroidMemberCount[i];
            if(centroidMembershipCount != 0){
                for(int j=0; j<numCol; j++){
                    newCentroids[i][j] = newCentroids[i][j]/(double)centroidMembershipCount;
                }
            }
            
        }
        
        return newCentroids;
    }
    private static void writeResultsToFile(double[][] centroids, int numCol) throws FileNotFoundException {
        String outPath = "finalCentroids.txt";
        PrintWriter centroidWriter = new PrintWriter(outPath);
        
        for(int i=0; i<centroids.length; i++){
            for(int j=0; j<numCol; j++){
                centroidWriter.print(centroids[i][j] + " ");
            }
            centroidWriter.println();
        }
        
        centroidWriter.flush();
        centroidWriter.close();
    }
    
    public static void main(String args[]) throws InterruptedException, FileNotFoundException {
        long startTime = System.currentTimeMillis();
        final int numIteration = Integer.parseInt(args[0]);
        final String dataFileName = args[1].trim();
        final int numRow = Integer.parseInt(args[2]);
        final int numCol = Integer.parseInt(args[3]);
        final int k = Integer.parseInt(args[4]);
//        final int k = 5, numRow = 40, numCol = 10, numIteration=5;
//        final String dataFileName = "a.txt";
//        R1.setArray(10);
//        int[] retArray = R1.getArray();
//        for(int i=0; i<5; i++){
//            System.out.println(retArray[i]);
//        }

        double[][] matrix = readData(numRow, numCol, dataFileName);
        double[][] centroids = getCentoids(matrix, k, numCol);
        
        
        for(int iter=0; iter<numIteration; iter++){       
//            System.out.println("\nBegining of Iteration: "+iter);dumpMatrix(centroids, k, numCol);

            double[][] newCentroids = new double[k][numCol];
            int[] newCentroidMemberCount = new int[k];

            //it should be 0-2 2-4 4-6
            int minIndex1 = 0, maxIndex1 = 1430;
            int minIndex2 = 1430, maxIndex2 = 2860;
            int minIndex3 = 2860, maxIndex3 = 4290;
            int minIndex4 = 4290, maxIndex4 = 5720;
            int minIndex5 = 5720, maxIndex5 = 7150;
            int minIndex6 = 7150, maxIndex6 = 8580;
            int minIndex7 = 8580, maxIndex7 = 10010;
            int minIndex8 = 10010, maxIndex8 = 11440;
            int minIndex9 = 11440, maxIndex9 = 12870;
            int minIndex10 = 12870, maxIndex10 = 14300;
            int minIndex11 = 14300, maxIndex11 = 15730;
            int minIndex12 = 15730, maxIndex12 = 17160;
            int minIndex13 = 17160, maxIndex13 = 18590;
            int minIndex14 = 18590, maxIndex14 = 19997;
            
            RunnableDemo R1 = new RunnableDemo( "Thread-1");
            //remember to call setCentroids at the beginning, k and numCol sets there
            R1.setCentroids(k, numCol, centroids);
            R1.setData(minIndex1, maxIndex1, numCol, matrix);
            R1.start();

            RunnableDemo R2 = new RunnableDemo( "Thread-2");                
            R2.setCentroids(k, numCol, centroids);
            R2.setData(minIndex2, maxIndex2, numCol, matrix);
            R2.start();

            RunnableDemo R3 = new RunnableDemo( "Thread-3");                
            R3.setCentroids(k, numCol, centroids);
            R3.setData(minIndex3, maxIndex3, numCol, matrix);
            R3.start();

            RunnableDemo R4 = new RunnableDemo( "Thread-4");                
            R4.setCentroids(k, numCol, centroids);
            R4.setData(minIndex4, maxIndex4, numCol, matrix);
            R4.start();

            RunnableDemo R5 = new RunnableDemo( "Thread-5");                
            R5.setCentroids(k, numCol, centroids);
            R5.setData(minIndex5, maxIndex5, numCol, matrix);
            R5.start();
            
            RunnableDemo R6 = new RunnableDemo( "Thread-6");                
            R6.setCentroids(k, numCol, centroids);
            R6.setData(minIndex6, maxIndex6, numCol, matrix);
            R6.start();
            
            RunnableDemo R7 = new RunnableDemo( "Thread-7");                
            R7.setCentroids(k, numCol, centroids);
            R7.setData(minIndex7, maxIndex7, numCol, matrix);
            R7.start();
            
            RunnableDemo R8 = new RunnableDemo( "Thread-8");                
            R8.setCentroids(k, numCol, centroids);
            R8.setData(minIndex8, maxIndex8, numCol, matrix);
            R8.start();
            
            RunnableDemo R9 = new RunnableDemo( "Thread-9");                
            R9.setCentroids(k, numCol, centroids);
            R9.setData(minIndex9, maxIndex9, numCol, matrix);
            R9.start();
            
            RunnableDemo R10 = new RunnableDemo( "Thread-10");                
            R10.setCentroids(k, numCol, centroids);
            R10.setData(minIndex10, maxIndex10, numCol, matrix);
            R10.start();
            
            RunnableDemo R11 = new RunnableDemo( "Thread-11");                
            R11.setCentroids(k, numCol, centroids);
            R11.setData(minIndex11, maxIndex11, numCol, matrix);
            R11.start();
            
            RunnableDemo R12 = new RunnableDemo( "Thread-12");                
            R12.setCentroids(k, numCol, centroids);
            R12.setData(minIndex12, maxIndex12, numCol, matrix);
            R12.start();
            
            RunnableDemo R13 = new RunnableDemo( "Thread-13");                
            R13.setCentroids(k, numCol, centroids);
            R13.setData(minIndex13, maxIndex13, numCol, matrix);
            R13.start();
            
            RunnableDemo R14 = new RunnableDemo( "Thread-14");                
            R14.setCentroids(k, numCol, centroids);
            R14.setData(minIndex14, maxIndex14, numCol, matrix);
            R14.start();
            
            
            
            
            
            double[][] r1Distance = R1.getDistanceFromThread();
            int[] closestCentroids1 = getClosestCentroid(r1Distance, (maxIndex1-minIndex1), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids1, minIndex1, maxIndex1, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids1);

            double[][] r2Distances = R2.getDistanceFromThread();
            int[] closestCentroids2 = getClosestCentroid(r2Distances, (maxIndex2-minIndex2), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids2, minIndex2, maxIndex2, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids2);

            double[][] r3Distances = R3.getDistanceFromThread();
            int[] closestCentroids3 = getClosestCentroid(r3Distances, (maxIndex3-minIndex3), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids3, minIndex3, maxIndex3, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids3);

            double[][] r4Distances = R4.getDistanceFromThread();
            int[] closestCentroids4 = getClosestCentroid(r4Distances, (maxIndex4-minIndex4), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids4, minIndex4, maxIndex4, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids4);
            
            double[][] r5Distances = R5.getDistanceFromThread();
            int[] closestCentroids5 = getClosestCentroid(r5Distances, (maxIndex5-minIndex5), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids5, minIndex5, maxIndex5, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids5);
            
            double[][] r6Distances = R6.getDistanceFromThread();
            int[] closestCentroids6 = getClosestCentroid(r6Distances, (maxIndex6-minIndex6), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids6, minIndex6, maxIndex6, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids6);
            
            double[][] r7Distances = R7.getDistanceFromThread();
            int[] closestCentroids7 = getClosestCentroid(r7Distances, (maxIndex7-minIndex7), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids7, minIndex7, maxIndex7, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids7);
            
            double[][] r8Distances = R8.getDistanceFromThread();
            int[] closestCentroids8 = getClosestCentroid(r8Distances, (maxIndex8-minIndex8), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids8, minIndex8, maxIndex8, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids8);
            
            double[][] r9Distances = R9.getDistanceFromThread();
            int[] closestCentroids9 = getClosestCentroid(r9Distances, (maxIndex9-minIndex9), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids9, minIndex9, maxIndex9, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids9);
            
            double[][] r10Distances = R10.getDistanceFromThread();
            int[] closestCentroids10 = getClosestCentroid(r10Distances, (maxIndex10-minIndex10), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids10, minIndex10, maxIndex10, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids10);
            
            double[][] r11Distances = R11.getDistanceFromThread();
            int[] closestCentroids11 = getClosestCentroid(r11Distances, (maxIndex11-minIndex11), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids11, minIndex11, maxIndex11, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids11);
            
            double[][] r12Distances = R12.getDistanceFromThread();
            int[] closestCentroids12 = getClosestCentroid(r12Distances, (maxIndex12-minIndex12), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids12, minIndex12, maxIndex12, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids12);
            
            double[][] r13Distances = R13.getDistanceFromThread();
            int[] closestCentroids13 = getClosestCentroid(r13Distances, (maxIndex13-minIndex13), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids13, minIndex13, maxIndex13, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids13);
            
            double[][] r14Distances = R14.getDistanceFromThread();
            int[] closestCentroids14 = getClosestCentroid(r14Distances, (maxIndex14-minIndex14), k);
            newCentroids = addDataPointsToNewCentroid(matrix, newCentroids, closestCentroids14, minIndex14, maxIndex14, numCol);
            newCentroidMemberCount = addToNewCentroidMemberCount(newCentroidMemberCount, closestCentroids14);


            //end of iteration, divide the added new centroid by count to get updated centroid
            newCentroids = divideCentroidByCount(newCentroids, newCentroidMemberCount, k, numCol);
            centroids  = newCentroids; // assigning the new centroids as original centroids for the next iteration.
            System.out.println("\nEnd of iteration: "+iter);
//            dumpMatrix(centroids, k, numCol);
//            dumpArray(newCentroidMemberCount);
        
        }
        
        writeResultsToFile(centroids, numCol);
        System.out.println("\n\nK-means finished running!!\nThe resulting centroids are written to finalCentroids.txt file");
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("\n\nTotal time taken: " + totalTime + "\n");
        
    }

    
}