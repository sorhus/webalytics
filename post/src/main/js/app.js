const React = require("react");
const ReactDOM = require('react-dom');
const $ = require("jquery");

class Dimension extends React.Component {
    render() {
        var rows = [];
        for (var i = 0; i < this.props.values.length; i++) {
            rows.push(<li>{this.props.values[i]}</li>);
        }
        return (
            <li>{this.props.name}:
                <ul>
                    {rows}
                </ul>
            </li>
        );
    }
}

class Domain extends React.Component {
    render() {
        var rows = [];
        for(var dimension in this.props.domain) {
            rows.push(<Dimension key={dimension} name={dimension} values={this.props.domain[dimension]} />)
        }
        return (
            <ul>
                {rows}
            </ul>
        )
    }
}


class CountRow extends React.Component {
    render() {
        return (
            <tr>
                <td>{this.props.bucket}</td>
                <td>{this.props.dimension}</td>
                <td>{this.props.value}</td>
                <td>{this.props.count}</td>
            </tr>
        );
    }
}


class CountTable extends React.Component {
    render() {
        console.log(this.props);
        var rows = [];
        for(var bucket in this.props.data) {
            for(var dimension in this.props.data[bucket]) {
                for (var value in this.props.data[bucket][dimension]) {
                    rows.push(<CountRow key={bucket,dimension,value} bucket={bucket} dimension={dimension} value={value}
                                        count={this.props.data[bucket][dimension][value]}/>)
                }
            }
        }
        return (
            <table>
                <thead>
                    <tr>
                        <th>Bucket</th>
                        <th>Dimension</th>
                        <th>Value</th>
                        <th>Count</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        )
    }
}

const CountApp = React.createClass({

    render() {
        return (
            <div>
                <h1>Counts</h1>
                <CountTable key="data" data={this.state.count}/>
            </div>
        )
    },

    getInitialState() {
        return {count: {}};
    },

    componentDidMount() {
        $.ajax({
            url: 'count/all',
            type: 'POST',
            data: {count: {}}
        }).done(json => {
            console.log(json);
            this.setState({count: json});
        });
    }

});

const DomainApp = React.createClass({

    render() {
        console.log(this.state.domain);
        return (
            <div>
                <h1>Domain</h1>
                <Domain key="domain" domain={this.state.domain}/>
            </div>
        )
    },

    getInitialState() {
        return {domain: {}};
    },

    componentDidMount() {
        $.ajax({
            url: 'domain',
            type: 'GET',
            data: {domain: {}}
        }).done(json => {
            this.setState({domain: json});
        });
    }
});


const App = React.createClass({

    render() {
        return (
            <div>
                <DomainApp />
                <br></br>
                <CountApp />
            </div>
        )
    }
});

ReactDOM.render(
    // {/*<DomainApp/>,*/}
    // <CountApp/>,
    <App/>,
    document.getElementById('app')
);