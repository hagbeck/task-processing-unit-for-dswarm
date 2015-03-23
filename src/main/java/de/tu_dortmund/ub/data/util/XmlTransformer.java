/*
The MIT License (MIT)

Copyright (c) 2015, Hans-Georg Becker, http://orcid.org/0000-0003-0432-294X

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

package de.tu_dortmund.ub.data.util;

import net.sf.saxon.s9api.*;
import org.jdom2.Document;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.jdom2.transform.JDOMSource;

import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.util.HashMap;

/**
 * Utility Class for processing XSLT stylesheets
 *
 * @author Dipl.-Math. Hans-Georg Becker (M.L.I.S.)
 * @version 2015-03-19
 *
 */
public class XmlTransformer {

    public static void main(String[] args) throws Exception {

        Document document = new SAXBuilder().build(new File("data/project.mods.xml"));

        XMLOutputter out = new XMLOutputter(Format.getPrettyFormat());

        BufferedWriter bufferedWriter = null;
        try {

            bufferedWriter = new BufferedWriter(new FileWriter("data/cdata.project.mods.xml"));

            out.output(new SAXBuilder().build(new StringReader(xmlOutputter(document, "xslt/cdata.xsl", null))),bufferedWriter);
        }
        finally {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
        }

    }

    public static String xmlOutputter(Document doc, String xslt, HashMap<String,String> params) throws IOException {

        String result = null;

        try {

            Processor processor = new Processor(false);

            XdmNode source = processor.newDocumentBuilder().build(new JDOMSource( doc ));
            Serializer out = new Serializer();
            out.setOutputProperty(Serializer.Property.METHOD, "xml");
            out.setOutputProperty(Serializer.Property.INDENT, "yes");

            StringWriter buffer = new StringWriter();
            out.setOutputWriter(new PrintWriter( buffer ));

            XsltCompiler xsltCompiler = processor.newXsltCompiler();
            XsltExecutable exp = xsltCompiler.compile(new StreamSource(xslt));
            XsltTransformer trans = exp.load();
            trans.setInitialContextNode(source);
            trans.setDestination(out);

            if (params != null) {
                for (String p : params.keySet()) {
                    trans.setParameter(new QName(p), new XdmAtomicValue(params.get(p)));
                }
            }

            trans.transform();

            result = buffer.toString();

        } catch (SaxonApiException e) {

            e.printStackTrace();
        }

        return result;
    }

}
