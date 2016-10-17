import {Component, Input, ElementRef, Inject}      from '@angular/core';

import 'rxjs/Rx';

import { CandidatesService } from "../services/api";


@Component({
    selector: 'smt-alignment-result',
    templateUrl: '../../templates/alignment-result.html',
    providers: [ ]
})
export class AlignmentResult {
    @Input('annotation') private annotation;

    getClassName() {
        switch(this.annotation.nerClass) {
            case "PERSON":
                return "label label-success";
            case "ORGANIZATION":
                return "label label-primary";
            default:
                return "";
        }
    }

    hasAlignment() {
        return typeof this.annotation.alignment != 'undefined' && typeof this.annotation.user != 'undefined';
    }
}

@Component({
    selector: 'smt-annotate',
    templateUrl: '../../templates/annotate.html',
    providers: [ CandidatesService ],
    viewProviders: [ AlignmentResult ]
})
export class Annotate {
    private errorMessage : string;
    private result = null;
    private users = {};

    annotate(searchString : string) {
        let self = this;
        this.candidatesService.getAnnotation(searchString).subscribe(
            function (annotation) {
                self.result = annotation.annotations;
                for (var user of annotation.users) {
                    self.users[user.screenName] = user;
                }
                for (var token of self.result) {
                    token.show = false;
                    if (typeof token.alignment != 'undefined') {
                        let candidates = token.alignment.candidates;
                        let topScore = 0.0;
                        let topCandidate = "";
                        for (var username of Object.keys(candidates)) {
                            if (candidates[username] > topScore) {
                                topScore = candidates[username];
                                topCandidate = username;
                            }
                        }
                        token.user = self.users[topCandidate];
                    }
                }
            },
            function (error) {
                self.result = null;
                self.errorMessage = <any>error;
            }
        );
    }

    constructor(private candidatesService : CandidatesService) { };
}