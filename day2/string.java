import java.lang.String;
import java.util.StringTokenizer;
class abc {
 static boolean isNumber(String s)
  {
  for (int i=0;i < s.length(); i++)
   {
     char ch=s.charAt(i);
     if(ch<'0' || ch>'9')
      return false;
   }
   return true;
  }
  

 public static void main(String args[])
 {
 String s = " 1st and 3nd they are 21 and 22 yrs old";
 StringTokenizer st = new StringTokenizer(s);
  while (st.hasMoreTokens())
  {
     
     String d = st.nextToken();
     if(isNumber(d))
 
     System.out.println(d);
     }
     }
     }
