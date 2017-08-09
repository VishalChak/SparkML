package ml.utility;



/*import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import okhttp3.Credentials;
import okhttp3.HttpUrl;

public class WatsonApiTranslator {

	public static void main(String[] args) throws Exception {
		WatsonApiTranslator watsonApiTranslator = new WatsonApiTranslator();
		final String URL = "https://gateway.watsonplatform.net/language-translator/api/v2/";
		String apiKey = Credentials.basic("c34588ba-7bb6-463a-9626-464d223a1ff8", "X4nAR7L53VrM");
		String text = "Comment allez-vous?";
		System.out.println("API KEY <<<" + apiKey);

		HttpUrl url = HttpUrl.parse(URL).newBuilder().addPathSegment("identify").addQueryParameter("text", text)
				.build();

		Map<String, Object> map = new HashMap();
		map.put("Authorization", apiKey);
		System.out.println("URL >>>" + url.toString());
		String language = watsonApiTranslator.doPostRequest(url.toString(), map);

		System.out.println(language);
		if (!(language.equalsIgnoreCase("en"))) {
			HttpUrl urlForTranslation = HttpUrl.parse(URL).newBuilder().addPathSegment("translate")
					.addQueryParameter("text", text).addQueryParameter("source", language)
					.addQueryParameter("target", "en").build();
			String translatedText = watsonApiTranslator.doPostRequest(urlForTranslation.toString(), map);
			System.out.println(translatedText);
		}

	}

	public String doPostRequest(String url, Map<String, Object> requestParam) throws Exception {
		URL obj = new URL(url);
		URLConnection openConnection = obj.openConnection();

		if (requestParam != null) {
			Set<String> keySet = requestParam.keySet();
			if (keySet != null && keySet.size() > 0) {
				for (String key : keySet) {
					openConnection.addRequestProperty(key, requestParam.get(key).toString());
				}
			}
		}
		openConnection.setDoOutput(true);

		OutputStream outputStream = openConnection.getOutputStream();

		if (outputStream != null) {
			return readInputStreamAndGenerateOutput(openConnection.getInputStream());
		}
		return null;
	}


	public static String readInputStreamAndGenerateOutput(InputStream inputStream) throws IOException {

		BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		return response.toString();
	}
}
*/