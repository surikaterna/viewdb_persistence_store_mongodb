var should = require("should");
var expect = require("mocha").expect;
var projectDocument = require("../lib/utils").projectDocument;
var _ = require("lodash");

describe("Project document", function () {
  it("#Should pick property", function (done) {
    var result = projectDocument({ alfa: "abc" }, { alfa: 1 });

    result.should.eql({ alfa: "abc" });
    done();
  });

  it("#Should pick property from nested object", function (done) {
    var result = projectDocument(
      { alfa: { bravo: { charlie: 1, delta: 2 }, echo: 3 } },
      { alfa: { bravo: { delta: 1 } } }
    );

    result.should.eql({ alfa: { bravo: { delta: 2 } } });
    done();
  });

  it("#Should pick property from nested array", function (done) {
    var result = projectDocument(
      {
        alfa: {
          bravo: [
            { delta: 1, echo: 1 },
            { delta: 2, echo: 2 },
          ],
        },
      },
      { alfa: { bravo: { delta: 1 } } }
    );

    result.should.eql({ alfa: { bravo: [{ delta: 1 }, { delta: 2 }] } });

    done();
  });

  it("#Should pick nested property from nested array", function (done) {
    var result = projectDocument(
      {
        alfa: {
          bravo: [
            { delta: 1, echo: { foxtrot: 2, gemini: 3 } },
            { delta: 4, echo: { foxtrot: 5, gemini: 6 } }
          ]
        }
      },
      { alfa: { bravo: { echo: { gemini: 1 } } } }
    );
    result.should.eql({ alfa: { bravo: [ { echo: { gemini: 3 } }, { echo: { gemini: 6 } } ] } });

    done();
  });

  it("#Should support exclude paths", function (done) {
    var result = projectDocument(
      { alfa: 1, bravo: 2, charlie: 3, delta: 4 },
      { bravo: 0 }
    );

    result.should.eql({ alfa: 1, charlie: 3, delta: 4 });
    done();
  });

  it("#Should support multiple exclude paths", function (done) {
    var result = projectDocument(
      { alfa: 1, bravo: 2, charlie: 3, delta: { echo: 4, foxtrot: 5 } },
      { bravo: 0, delta: { echo: 0 } }
    );

    result.should.eql({ alfa: 1, charlie: 3, delta: { foxtrot: 5 } });
    done();
  });
});
