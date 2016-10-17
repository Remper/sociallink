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