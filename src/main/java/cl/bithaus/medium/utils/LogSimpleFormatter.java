/*
 * Copyright (c) BitHaus Software Chile
 * All rights reserved. www.bithaus.cl.
 * 
 * All rights to this product are owned by Bithaus Chile and may only by used 
 * under the terms of its associated license document. 
 * You may NOT copy, modify, sublicense or distribute this source file or 
 * portions of it unless previously authorized by writing by Bithaus Software Chile.
 * In any event, this notice must always be included verbatim with this file.
 */
package cl.bithaus.medium.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 *
 * @author jmakuc
 */
public class LogSimpleFormatter extends Formatter{
    

    @Override
    public String format(LogRecord record) {

        String str = "";
        str += new SimpleDateFormat("yyyy-MM-dd H:mm:ss.SSS").format(new Date(record.getMillis())) + " ";        
        str += "[" + record.getLevel() + "] ";
        str += record.getLoggerName() + " ";
        str += record.getMessage();

        if(record.getThrown() != null) {

            StringWriter sw = null;
            PrintWriter pw = null;
            try {
             sw = new StringWriter();
             pw = new PrintWriter(sw);
             record.getThrown().printStackTrace(pw);
             str += "\n" + sw.toString();
            } finally {
             try {
               if(pw != null)  pw.close();
               if(sw != null)  sw.close();
             } catch (IOException ignore) {}
            }
        }

        str += "\n";

        return str;
    }


}
