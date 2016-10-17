import { Http, Response }    from '@angular/http';
import { Injectable }        from '@angular/core';

import 'rxjs/Rx';
import { Observable }        from 'rxjs/Observable';

@Injectable()
export class WikimachineService {
    constructor (private http: Http) {}

    private endpoint = 'http://ml.apnetwork.it/annotate';

    getAnnotation(searchString : String): Observable<any[]> {
        let body = {
            'text': searchString,
            'disambiguation': 1,
            'topic': 1,
            'include_text': 1,
            'min_weight': 0.25,
            'image': 1,
            'class': 1,
            'app_id': 0,
            'app_key': 0
        };
        return this.http.post(this.endpoint, body)
            .map(WikimachineService.extractData)
            .catch(WikimachineService.handleError);
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