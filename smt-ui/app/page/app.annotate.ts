import { Component }      from '@angular/core';

import 'rxjs/Rx';

import { WikimachineService } from "../services/wikimachine-api";

@Component({
    selector: 'smt-annotate',
    templateUrl: '../../templates/annotate.html',
    providers: [ WikimachineService ]
})
export class Annotate {
    private errorMessage : string;
    private result = null;

    annotate(searchString : string) {
        this.result = searchString;

        let self = this;
        this.wikimachineService.getAnnotation(searchString).subscribe(
            function (annotation) {
                console.log(annotation);
            },
            function (error) {
                self.errorMessage = <any>error;
            }
        );
    }

    constructor(private wikimachineService : WikimachineService) {
    };
}