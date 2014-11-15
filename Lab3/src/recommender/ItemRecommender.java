package recommender;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 * @author shubham
 *
 */

public class ItemRecommender {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			DataModel model = new FileDataModel(new File("data/ratings.csv"));
			ItemSimilarity item = new LogLikelihoodSimilarity(model);
			//TanimotoCoefficientSimilarity item = new TanimotoCoefficientSimilarity(model);
			
			
			GenericItemBasedRecommender gm = new GenericItemBasedRecommender(model, item);

			int x=1;
			BufferedWriter bw = new BufferedWriter(new FileWriter("output/itemrecommendation.csv"));
			for(LongPrimitiveIterator prim = model.getItemIDs(); prim.hasNext();){
				long itemid = prim.nextLong();
				List<RecommendedItem> listrecomm = gm.mostSimilarItems(itemid, 3);
				
				for(RecommendedItem recommendation : listrecomm){
					System.out.println(itemid+","+recommendation.getItemID()+","+recommendation.getValue());
					bw.write(itemid+","+recommendation.getItemID()+","+recommendation.getValue()+"\n");
				}
				x++;
				//if(x>10) System.exit(1);
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("There was an error.");
			e.printStackTrace();
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			System.out.println("There was a taste error.");
			e.printStackTrace();
		}

	}

}
