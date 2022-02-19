package betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import betterreadsdataloader.author.Author;
import betterreadsdataloader.author.AuthorRepository;
import betterreadsdataloader.connection.DataStaxConnectionProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxConnectionProperties.class)
public class BetterreadsDataLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);

	}

    private void initAuthors(){
       Path path = Paths.get(authorDumpLocation);
       try (Stream<String> lines = Files.lines(path)) {
           lines.forEach(line ->{
               //read and parse the line
               String jsonString = line.substring(line.indexOf("{"));
               JSONObject jsonObject = null;
               try {
                    jsonObject = new JSONObject(jsonString);
            } catch (JSONException e) {
                e.printStackTrace();
            }
               //construct author object
               Author author = new Author();
            author.setName(jsonObject.optString("name"));
            author.setPersonalName(jsonObject.optString("personal_name"));
            author.setId(jsonObject.optString("key").replace("/authors/", ""));

               //persist using repository
               authorRepository.save(author);
           });
    } catch (IOException e) {
        e.printStackTrace();
    }

    }

    private void initWorks(){

    }

    @PostConstruct
    public void start() throws JSONException{
        initAuthors();
        //initWorks();
    }




	@Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxConnectionProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
