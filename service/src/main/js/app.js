const React = require("react");
const ReactDOM = require('react-dom');
const $ = require("jquery");

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
                    rows.push(<CountRow key={bucket + dimension + value} bucket={bucket} dimension={dimension} value={value}
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
                <button className="square" onClick={() => alert('click')}/>
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


const App = React.createClass({

    render() {
        return (
            <div>
                <CountApp />
            </div>
        )
    }
});

ReactDOM.render(
    <App/>,
    document.getElementById('app')
);