package report


import "testing"


const checkMark =  "\u2713"
const ballotX = "\u2717"

func TestFunction(t *testing.T) {
	t.Log("Given the need to return a csv writer")
	{
		t.Logf("checking for function(getWriter) return")
		{
			_,err := getwriter()
			if err != nil  {
				t.Fatal("\t Should have been a *csv.Writer",ballotX)
			}
			t.Log("Got the Csv Writer", checkMark)
		}
	}
	t.Log("Given the need for elastic client")
	{
		t.Logf("checking return of function getClient")
		{
			_,err := getClient()
			if err != nil {
				t.Fatal("Should have been a elastic search client",ballotX)
			}
			t.Log("Got the elastic search client",checkMark)
		}
	}
}
