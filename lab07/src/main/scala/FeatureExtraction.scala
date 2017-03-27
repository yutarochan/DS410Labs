package lab7

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.io.Source._

object FeatureExtraction extends Serializable {
    def constructFeatureVectorsFromPapers(papers: RDD[Map[String, Array[String]]]): RDD[Vector] = {
        val wordsOfPapers = papers.map(getWordsOfPaper)
        return constructFeatureVectors(wordsOfPapers)
    }

    def constructFeatureVectors(wordsOfPapers: RDD[Iterable[String]]): RDD[Vector] = {
        val hashingTF = new HashingTF()
        val tfVectors = hashingTF.transform(wordsOfPapers)
        val idfModel = new IDF().fit(tfVectors)
        val tfidfVectors = idfModel.transform(tfVectors)
        return tfidfVectors
    }

    def getWordsOfPaper(paper: Map[String, Array[String]]): Iterable[String] = {
        val abstracts = paper.getOrElse("A", Array())
        val words = abstracts.flatMap(s => s.split("[ ,.;()]").toIterable)  // Note: parameter for split is regex
        val stopWords = List("", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
		"a", "about", "above", "after", "again", "against", "all", "am",
		"an", "and", "any", "are", "aren\'t", "as", "at", "be", "because",
		"been", "before", "being", "below", "between", "both", "but",
		"by", "can\'t", "cannot", "could", "couldn\'t", "did", "didn\'t",
		"do", "does", "doesn\'t", "doing", "don\'t", "down", "during",
		"each", "few", "for", "from", "further", "had", "hadn\'t", "has",
		"hasn\'t", "have", "haven\'t", "having", "he", "he\'d", "he\'ll", "he\'s",
		"her", "here", "here\'s", "hers", "herself", "him", "himself", "his",
		"how", "how\'s", "i", "i\'d", "i\'ll", "i\'m", "i\'ve", "if", "in", "into",
		"is", "isn\'t", "it", "it\'s", "its", "itself", "let\'s", "me", "more",
		"most", "mustn\'t", "my", "myself", "no", "nor", "not", "of", "off", "on",
		"once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out",
		"over", "own", "same", "shan\'t", "she", "she\'d", "she\'ll", "she\'s",
		"should", "shouldn\'t", "so", "some", "such", "than", "that", "that\'s",
		"the", "their", "theirs", "them", "themselves", "then", "there", "there\'s",
		"these", "they", "they\'d", "they\'ll", "they\'re", "they\'ve", "this", "those",
		"through", "to", "too", "under", "until", "up", "very", "was", "wasn\'t", "we",
		"we\'d", "we\'ll", "we\'re", "we\'ve", "were", "weren\'t", "what", "what\'s",
		"when", "when\'s", "where", "where\'s", "which", "while", "who", "who\'s",
		"whom", "why", "why\'s", "with", "won\'t", "would", "wouldn\'t", "you", "you\'d",
		"you\'ll", "you\'re", "you\'ve", "your", "yours", "yourself", "yourselves")
	return words.filter(w => !stopWords.contains(w))
    }
}
