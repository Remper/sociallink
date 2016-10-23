import { Http, Response }    from '@angular/http';
import { Injectable }        from '@angular/core';

import 'rxjs/Rx';
import { Observable }        from 'rxjs/Observable';

@Injectable()
export class CandidatesService {
    constructor (private http: Http) {}
    private domainUrl = 'https://api.futuro.media/smt/';
    private profilesUrl = this.domainUrl+'profiles?name=';  // URL to web API
    private profilesTopicUrl = this.domainUrl+'profiles/topic?name=:name&topic=:topic';
    private annotateUrl = this.domainUrl+'annotate?text=';
    private annotateNerUrl = this.domainUrl+'annotate/ner?text=';
    private annotateTwitterUrl = this.domainUrl+'annotate/twitter?text=:text&token=:token&ner=:ner';

    getCandidates(searchString : String): Observable<any[]> {
        return this.http.get(this.profilesUrl+searchString)
            .map(CandidatesService.extractData)
            .catch(CandidatesService.handleError);
    }

    getCandidatesWithTopic(searchString : string, topic : string): Observable<any[]> {
        return this.http.get(this.profilesTopicUrl.replace(":name", searchString).replace(":topic", topic))
            .map(CandidatesService.extractData)
            .catch(CandidatesService.handleError);
    }

    getAnnotation(searchString : string): Observable<any> {
        return this.http.get(this.annotateUrl+searchString)
            .map(CandidatesService.extractData)
            .catch(CandidatesService.handleError);
    }

    getNerAnnotation(searchString : string): Observable<NerAnnotation[]> {
        return this.http.get(this.annotateNerUrl+searchString)
            .map(CandidatesService.extractData)
            .catch(CandidatesService.handleError);
    }

    getTwitterAnnotation(text : string, token : string, ner : string): Observable<TwitterAnnotation> {
        return this.http.get(this.annotateTwitterUrl
                .replace(":text", text)
                .replace(":token", token)
                .replace(":ner", ner)
            )
            .map(CandidatesService.extractData)
            .map(function(rawObject) {
                let annotation = new TwitterAnnotation();
                annotation.candidates = rawObject.candidates;
                annotation.results = rawObject.results;
                annotation.token = new NerAnnotation();
                annotation.token.token = rawObject.token;
                annotation.token.nerClass = rawObject.nerClass;
                return annotation;
            })
            .catch(CandidatesService.handleError);
    }

    private static extractData(res: Response) {
        return res.json().data || [];
    }

    private static handleError (error: any) {
        // In a real world app, we might use a remote logging infrastructure
        // We'd also dig deeper into the error to get a better message
        let errMsg = (error.message) ? error.message :
            error.status ? `${error.status} - ${error.statusText}` : 'Server error';
        console.error(errMsg); // log to console instead
        return Observable.throw(errMsg);
    }
}

export class TwitterAnnotation {
    token : NerAnnotation;
    candidates : Object;
    results : ScoreBundle[];
}

export class ScoreBundle {
    type : string;
    scores : Score[];
}

export class Score {
    username : string;
    score : number;
    debug : Object;
}

export class NerAnnotation {
    token : string;
    nerClass : string;
}