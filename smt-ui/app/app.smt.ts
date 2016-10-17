import { NgModule, Component }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { Navigation, TabBarElement }    from './app.navigation';
import { CurrentUser }   from './app.cur.user';
import { Profiles }      from './page/app.profiles';
import { Annotate, AlignmentResult }      from "./page/app.annotate";
import { HttpModule }    from "@angular/http";

@Component({
    selector: 'body',
    template: '' +
        '<smt-navigation #tab></smt-navigation>' +
        '<smt-profiles *ngIf="tab.currentTab == \'profiles\'"></smt-profiles>' +
        '<smt-annotate *ngIf="tab.currentTab == \'annotate\'"></smt-annotate>'
})
class CoreDirective { }

@NgModule({
    imports:      [ BrowserModule, HttpModule ],
    declarations: [ CoreDirective, Navigation, CurrentUser, Profiles, Annotate, TabBarElement, AlignmentResult ],
    bootstrap:    [ CoreDirective ]
})
export class SMT { }