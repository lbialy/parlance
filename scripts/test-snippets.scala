//> using scala 3.8.2
//> using dep "com.kubuszok::scala-cli-md-spec:0.2.1"

import com.kubuszok.scalaclimdspec.*

val parlanceVersion = sys.env.getOrElse("PARLANCE_VERSION", "0.1.0-SNAPSHOT")

def replaceVersion(content: Snippet.Content): Snippet.Content = content match
  case Snippet.Content.Single(text) =>
    Snippet.Content.Single(text.replace("$VERSION$", parlanceVersion))
  case Snippet.Content.Multiple(files) =>
    Snippet.Content.Multiple(files.map { case (name, single) =>
      name -> replaceVersion(single).asInstanceOf[Snippet.Content.Single]
    })

@main def run(args: String*): Unit = testSnippets(args.toArray) { cfg =>
  new Runner.Default(cfg) {
    extension (snippet: Snippet)
      override def adjusted: Snippet =
        Snippet(snippet.locations, replaceVersion(snippet.content))
  }
}
