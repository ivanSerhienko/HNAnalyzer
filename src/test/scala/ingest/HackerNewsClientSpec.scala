package ingest

import zio._
import zio.http._
import zio.json._
import zio.test._
import zio.test.Assertion._

object HackerNewsClientSpec extends ZIOSpecDefault {
  def spec = suite("HackerNewsClient")(
    test("handleResponse fails on a non-2xx response") {
      val response = Response(status = Status.TooManyRequests)
      assertZIO(HackerNewsClient.handleResponse(response).exit)(
        fails(hasMessage(containsString("429"))),
      )
    },
    test("decodes a story item") {
      val json =
        """{"by":"Ariarule","descendants":91,"id":49347543,"kids":[49376987,49377676],"score":258,"time":1787068252,"title":"I like 'em thick: an apology to my English teachers","type":"story","url":"https://www.experimental-history.com/p/i-like-em-thick"}"""

      val expected = Item(
        id = 49347543,
        `type` = Some("story"),
        by = Some("Ariarule"),
        time = Some(1787068252L),
        url = Some("https://www.experimental-history.com/p/i-like-em-thick"),
        score = Some(258),
        title = Some("I like 'em thick: an apology to my English teachers"),
        descendants = Some(91),
        kids = Some(List(49376987L, 49377676L)),
      )

      assertTrue(json.fromJson[Item] == Right(expected))
    },
    test("decodes a comment item with no score field and a parent") {
      val json =
        """{"by":"lordnacho","id":49376987,"kids":[49377360],"parent":49347543,"text":"As an old man...","time":1787244020,"type":"comment"}"""

      val expected = Item(
        id = 49376987,
        `type` = Some("comment"),
        by = Some("lordnacho"),
        time = Some(1787244020L),
        text = Some("As an old man..."),
        parent = Some(49347543L),
        kids = Some(List(49377360L)),
      )

      assertTrue(json.fromJson[Item] == Right(expected))
    },
    test("decodes an item with no kids key as kids = None") {
      val json =
        """{"by":"htunnicliff","descendants":0,"id":49377427,"score":2,"text":"...","time":1787246136,"title":"Ask HN: One SSH key or many SSH keys?","type":"story"}"""

      assertTrue(json.fromJson[Item].map(_.kids) == Right(None))
    },
    test("decodeJson fails with a descriptive error on malformed JSON") {
      assertZIO(HackerNewsClient.decodeJson[Item]("""{"id": "not-a-number"}""").exit)(
        fails(hasMessage(containsString("id"))),
      )
    },
    test("Listing.path returns the correct relative path per listing") {
      assertTrue(
        Listing.Top.path == "/topstories.json",
        Listing.New.path == "/newstories.json",
        Listing.Ask.path == "/askstories.json",
        Listing.Show.path == "/showstories.json",
      )
    },
  )
}
