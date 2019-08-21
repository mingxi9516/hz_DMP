import java.util.Arrays;

public class SortTest {
    public static void main(String[] args) {
        /**
         * 冒泡排序
         */
       int[] array = {23,54,65,3,5,2,87};
        for(int i=0; i<array.length-1;i++){
            for(int j=0; j< array.length-i-1;j++){
                if(array[j] > array[j+1]){
                    int num = array[j];
                    array[j]=array[j+1];
                    array[j+1]=num;
                }
            }
        }

        /**
         * 选择排序
         */
        for(int i=0;i<array.length;i++){
            for(int j=i+1;j<array.length;j++){
                if(array[i] > array[j]){
                    int num = array[i];
                    array[i] = array[j];
                    array[j] = num;
                }
            }
        }

        /**
         * 快速排序
         */
        int start = 0;
        int end = array.length-1;
        quickSort(array,0,array.length-1);




        /**
         * 二分查找法
         */

//        int[] array = {88,90,343,12,43,54,65,87};
//        Arrays.sort(array);
//        int num = 87;
//        int left = 0;
//        int right = array.length - 1;
//        while(left < right){
//            int mid = (left + right) /2;
//            if(num > array[mid]){
//                left = mid;
//            }else if(num < array[mid]){
//                right = mid;
//            }else{
//                System.out.println(mid);
//                break;
//            }
//        }


        for(int arr:array){
            System.out.print(arr+" ");
        }
    }

    public static void quickSort(int[] numbers, int start, int end) {
        if (start < end) {
            int base = numbers[start]; // 选定的基准值（第一个数值作为基准值）
            int temp; // 记录临时中间值
            int i = start, j = end;
            do {
                while ((numbers[i] < base) && (i < end))
                    i++;
                while ((numbers[j] > base) && (j > start))
                    j--;
                if (i <= j) {
                    temp = numbers[i];
                    numbers[i] = numbers[j];
                    numbers[j] = temp;
                    i++;
                    j--;
                }
            } while (i <= j);
            if (start < j)
                quickSort(numbers, start, j);
            if (end > i)
                quickSort(numbers, i, end);
        }
    }
}
