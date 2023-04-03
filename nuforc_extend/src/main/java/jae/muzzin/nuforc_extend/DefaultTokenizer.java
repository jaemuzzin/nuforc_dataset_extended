package jae.muzzin.nuforc_extend;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor;

/**
 *
 * @author Admin
 */
public class DefaultTokenizer implements SentencePreProcessor{

    @Override
    public String preProcess(String string) {
        return lemmatizeSentence(string);
    }

    StanfordCoreNLP pipeline;

    public DefaultTokenizer() {  // set up pipeline properties
        Properties props = new Properties();
        // set the list of annotators to run
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma");
        // build pipeline
        pipeline = new StanfordCoreNLP(props);
    }

    public List<String> tokenize(String s) {
        // create a document object
        CoreDocument document = pipeline.processToCoreDocument(s.toLowerCase());
        return document.tokens().stream()
                .map(t -> t.lemma()).filter(w -> !w.startsWith("<") && !w.startsWith("[") && !w.startsWith("http"))
                                .filter(w -> w.length() > 2).collect(Collectors.toList());
    }
    
    public String lemmatizeSentence(String s) {
        return tokenize(s).stream()
                .reduce((a,b) -> b==null ? a : (a + " " + b)).orElse("");
    }

}
